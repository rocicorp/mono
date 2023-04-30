import {RWLock} from '@rocicorp/lock';
import type {Hash} from '../hash.js';
import {joinIterables} from '../iterables.js';
import type {MaybePromise} from '../replicache.js';
import {promiseVoid} from '../resolved-promises.js';
import {getSizeOfValue} from '../size-of-value.js';
import {Chunk, ChunkHasher, createChunk} from './chunk.js';
import {
  HeadChange,
  RefCountUpdatesDelegate,
  computeRefCountUpdates,
} from './gc.js';
import {Read, Store, Write, mustGetChunk} from './store.js';

/**
 * Dag Store which lazily loads values from a source store and then caches
 * them in an LRU cache.  The memory cache for chunks from the source store
 * size is limited to `sourceCacheSizeLimit` bytes, and values are evicted in an
 * LRU fashion.  The purpose of this store is to avoid holding the entire client
 * view (i.e. the source store's content) in each tab's JavaScript heap.
 *
 * This store's heads are independent from the heads of source store, and are
 * only stored in memory.
 *
 * Chunks which are created via this store's {@link Write} transaction's
 * {@link createChunk} method are assumed to not be persisted to the source
 * store and thus are cached separately from the source store chunks.  These
 * memory-only chunks will not be evicted, and their sizes are not counted
 * towards the source chunk cache size.  A memory-only chunk will be deleted if
 * it is no longer reachable from one of this store's heads.
 *
 * Writes only manipulate the in memory state of this store and do not alter the
 * source store.  Thus values must be written to the source store through a
 * separate process (see {@link persist}).
 *
 * Intended use:
 * 1. source store is the 'perdag', a slower persistent store (i.e.
 *    dag.StoreImpl using a kv.IDBStore)
 * 2. this store's 'main' head is initialized to the hash of a chunk containing
 *    a commit in the source store
 * 3. reads lazily read chunks from the source store and cache them
 * 3. writes are initially made to this store with memory-only chunks
 * 4. writes are asynchronously persisted to the source store through a separate
 *    process (see {@link persist}}. This process gathers memory-only chunks
 *    from this store and then writes them to the source store.  It then informs
 *    this store that these chunks are no longer memory-only by calling
 *    {@link chunksPersisted}, which move these chunks
 *    to this store's LRU cache of source chunks (making them eligible for
 *    eviction).
 *
 * @param sourceStore Store to lazy load and cache values from.
 * @param sourceCacheSizeLimit Size limit in bytes for cache of chunks loaded
 * from `sourceStore`.  This size of a value is determined using
 * `getSizeOfValue`.  Keys do not count towards cache size.  Memory-only chunks
 * do not count towards cache size.
 * @param getSizeOfValue Function for measuring the size in bytes of a value.
 */
export class LazyStore implements Store {
  /**
   * This lock is used to ensure correct isolation of Reads and Writes.
   * Multiple Reads are allowed in parallel but only a single Write.  Reads and
   * Writes see an isolated view of the store (corresponding to the Serializable
   * level of transaction isolation defined in the SQL standard).
   *
   * To ensure these semantics the read lock must be acquired when a Read is
   * created and held til it is closed, and a Write lock must be acquired when a
   * Write is created and held til it is committed or closed.
   *
   * Code must have a read or write lock to
   * - read `_heads`
   * - read `_memOnlyChunks`
   * - read `_sourceStore`
   * - read and write `_sourceChunksCache`
   * - read and write `_refCounts`
   * - read and write `_refs`
   * and must have a write lock to
   * - write `_heads`
   * - write `_memOnlyChunks`
   */
  private readonly _rwLock = new RWLock();
  private readonly _heads = new Map<string, Hash>();
  private readonly _sourceStore: Store;
  private readonly _chunkHasher: ChunkHasher;
  private readonly _assertValidHash: (hash: Hash) => void;

  /** The following are protected so testing subclass can access. */
  protected readonly _memOnlyChunks = new Map<Hash, Chunk>();
  protected readonly _sourceChunksCache: ChunksCache;
  /**
   * Ref counts are maintained so that chunks which are unreachable
   * from this stores heads can be eagerly and deterministically deleted from
   * `this._memOnlyChunks` and `this._sourceChunksCache`.
   *
   * These ref counts are independent from `this._sourceStore`'s ref counts.
   * These ref counts are based on reachability from `this._heads`.
   * A chunk is deleted from `this._memOnlyChunks` or
   * `this._sourceChunksCache` (which ever it is in) when its ref count becomes
   * zero.
   * These ref counts count the refs in `this._heads` and `this._refs`.
   *
   * Not all reachable chunk's refs are included in `this._refs`, because this
   * would require loading all chunks reachable in the source store in a
   * non-lazy manner. `this._refs` contains the refs of all currently reachable
   * chunks that were ever in `this._memOnlyChunks` or
   * `this._sourceChunksCache` (even if they have been evicted).  A
   * chunk's ref information is lazily discovered and stored in `this._refs` and
   * counted in `this._refCounts`. A chunk's entries in `this._refs` and
   * `this._refCounts` are only deleted when a chunk is deleted due to it
   * becoming unreachable (it is not deleted if the chunk is evicted from the
   * source-store cache).
   *
   * The major implication of this lazy discovery of source store refs, is that
   * a reachable source store chunk may not be cached when loaded, because it is
   * not known to be reachable because some of the pertinent refs have not been
   * discovered.  However, in practice chunks are read by traversing the graph
   * starting from a head, and all pertinent refs are discovered as part of the
   * traversal.
   *
   * These ref counts can be changed in two ways:
   * 1. A LazyRead has a cache miss and loads a chunk from the source store that
   * is reachable from this._heads.  If this chunk's refs are not currently
   * counted, it will not have an entry in `this._refs`.  In this case, the
   * chunks refs will be put in `this._refs` and `this._refCounts` will be
   * updated to count them.
   * 2. A LazyWrite commit updates a head (which can result in increasing or
   * decreasing ref count) or puts a reachable chunk (either a `memory-only` or
   * `source` chunk) that references this hash (increasing ref count).  The
   * computation of these ref count changes is delegated to the
   * `computeRefCountUpdates` shared with dag.StoreImpl.  In order to
   * delegate determining reachability to `computeRefCountUpdates` and defer
   * this determination until commit time, LazyWrite treats cache misses
   * as a 'put' of the lazily-loaded chunk.
   *
   * A chunk's hash may have an entry in `this._refCounts` without that
   * chunk have ever been in `this._memOnlyChunks` or `this._sourceChunksCache`.
   * This is the case when a head or a reachable chunk that was ever in
   * `this._memOnlyChunks` or `this._sourceChunksCache` references a chunk
   * which is not currently cached (either because it has not been read, or
   * because it has been evicted).
   */
  protected readonly _refCounts = new Map<Hash, number>();
  protected readonly _refs = new Map<Hash, readonly Hash[]>();

  constructor(
    sourceStore: Store,
    sourceCacheSizeLimit: number,
    chunkHasher: ChunkHasher,
    assertValidHash: (hash: Hash) => void,
    getSizeOfChunk: (chunk: Chunk) => number = getSizeOfValue,
  ) {
    this._sourceChunksCache = new ChunksCache(
      sourceCacheSizeLimit,
      getSizeOfChunk,
      this._refCounts,
      this._refs,
    );
    this._sourceStore = sourceStore;
    this._chunkHasher = chunkHasher;
    this._assertValidHash = assertValidHash;
  }

  async read(): Promise<LazyRead> {
    const release = await this._rwLock.read();
    return new LazyRead(
      this._heads,
      this._memOnlyChunks,
      this._sourceChunksCache,
      this._sourceStore,
      release,
      this._assertValidHash,
    );
  }

  async write(): Promise<LazyWrite> {
    const release = await this._rwLock.write();
    return new LazyWrite(
      this._heads,
      this._memOnlyChunks,
      this._sourceChunksCache,
      this._sourceStore,
      this._refCounts,
      this._refs,
      release,
      this._chunkHasher,
      this._assertValidHash,
    );
  }

  close(): Promise<void> {
    return promiseVoid;
  }

  /**
   * Does not acquire any lock on the store.
   */
  isCached(chunkHash: Hash): boolean {
    return (
      this._sourceChunksCache.getWithoutUpdatingLRU(chunkHash) !== undefined
    );
  }

  withSuspendedSourceCacheEvictsAndDeletes<T>(
    fn: () => MaybePromise<T>,
  ): Promise<T> {
    return this._sourceChunksCache.withSuspendedEvictsAndDeletes(fn);
  }
}

export class LazyRead implements Read {
  protected readonly _heads: Map<string, Hash>;
  protected readonly _memOnlyChunks: Map<Hash, Chunk>;
  protected readonly _sourceChunksCache: ChunksCache;
  protected readonly _sourceStore: Store;
  private _sourceRead: Promise<Read> | undefined = undefined;
  private readonly _release: () => void;
  private _closed = false;
  readonly assertValidHash: (hash: Hash) => void;

  constructor(
    heads: Map<string, Hash>,
    memOnlyChunks: Map<Hash, Chunk>,
    sourceChunksCache: ChunksCache,
    sourceStore: Store,
    release: () => void,
    assertValidHash: (hash: Hash) => void,
  ) {
    this._heads = heads;
    this._memOnlyChunks = memOnlyChunks;
    this._sourceChunksCache = sourceChunksCache;
    this._sourceStore = sourceStore;
    this._release = release;
    this.assertValidHash = assertValidHash;
  }

  isMemOnlyChunkHash(hash: Hash): boolean {
    return this._memOnlyChunks.has(hash);
  }

  async hasChunk(hash: Hash): Promise<boolean> {
    return (await this.getChunk(hash)) !== undefined;
  }

  async getChunk(hash: Hash): Promise<Chunk | undefined> {
    const memOnlyChunk = this._memOnlyChunks.get(hash);
    if (memOnlyChunk !== undefined) {
      return memOnlyChunk;
    }
    let chunk = this._sourceChunksCache.get(hash);
    if (chunk === undefined) {
      chunk = await (await this.getSourceRead()).getChunk(hash);
      if (chunk !== undefined) {
        this._sourceChunksCache.put(chunk);
      }
    }
    return chunk;
  }

  mustGetChunk(hash: Hash): Promise<Chunk> {
    return mustGetChunk(this, hash);
  }

  getHead(name: string): Promise<Hash | undefined> {
    return Promise.resolve(this._heads.get(name));
  }

  release(): void {
    if (!this._closed) {
      this._release();
      this._sourceRead
        ?.then(read => read.release())
        // If creation of the read failed there is nothing to release.
        // Catch to avoid `Uncaught (in promise)` errors being reported.
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        .catch(_ => {});
      this._closed = true;
    }
  }

  get closed(): boolean {
    return this._closed;
  }

  protected getSourceRead(): Promise<Read> {
    if (!this._sourceRead) {
      this._sourceRead = this._sourceStore.read();
    }
    return this._sourceRead;
  }
}

export class LazyWrite
  extends LazyRead
  implements Write, RefCountUpdatesDelegate
{
  private readonly _refCounts: Map<Hash, number>;
  private readonly _refs: Map<Hash, readonly Hash[]>;
  private readonly _chunkHasher: ChunkHasher;
  protected readonly _pendingHeadChanges = new Map<string, HeadChange>();
  protected readonly _pendingMemOnlyChunks = new Map<Hash, Chunk>();
  protected readonly _pendingCachedChunks = new Map<
    Hash,
    {chunk: Chunk; size: number}
  >();
  private readonly _createdChunks = new Set<Hash>();

  constructor(
    heads: Map<string, Hash>,
    memOnlyChunks: Map<Hash, Chunk>,
    sourceChunksCache: ChunksCache,
    sourceStore: Store,
    refCounts: Map<Hash, number>,
    refs: Map<Hash, readonly Hash[]>,
    release: () => void,
    chunkHasher: ChunkHasher,
    assertValidHash: (hash: Hash) => void,
  ) {
    super(
      heads,
      memOnlyChunks,
      sourceChunksCache,
      sourceStore,
      release,
      assertValidHash,
    );
    this._refCounts = refCounts;
    this._refs = refs;
    this._chunkHasher = chunkHasher;
  }

  createChunk = <V>(data: V, refs: readonly Hash[]): Chunk<V> => {
    const chunk = createChunk(data, refs, this._chunkHasher);
    this._createdChunks.add(chunk.hash);
    return chunk;
  };

  putChunk<V>(c: Chunk<V>, size?: number): Promise<void> {
    const {hash, meta} = c;
    this.assertValidHash(hash);
    if (meta.length > 0) {
      for (const h of meta) {
        this.assertValidHash(h);
      }
    }
    if (this._createdChunks.has(hash) || this.isMemOnlyChunkHash(hash)) {
      this._pendingMemOnlyChunks.set(hash, c);
    } else {
      this._pendingCachedChunks.set(hash, {chunk: c, size: size ?? -1});
    }
    return promiseVoid;
  }

  async setHead(name: string, hash: Hash): Promise<void> {
    await this._setHead(name, hash);
  }

  async removeHead(name: string): Promise<void> {
    await this._setHead(name, undefined);
  }

  private async _setHead(name: string, hash: Hash | undefined): Promise<void> {
    const oldHash = await this.getHead(name);
    const v = this._pendingHeadChanges.get(name);
    if (v === undefined) {
      this._pendingHeadChanges.set(name, {new: hash, old: oldHash});
    } else {
      // Keep old if existing
      v.new = hash;
    }
  }

  override isMemOnlyChunkHash(hash: Hash): boolean {
    return (
      this._pendingMemOnlyChunks.has(hash) || super.isMemOnlyChunkHash(hash)
    );
  }

  override async getChunk(hash: Hash): Promise<Chunk | undefined> {
    const pendingMemOnlyChunk = this._pendingMemOnlyChunks.get(hash);
    if (pendingMemOnlyChunk !== undefined) {
      return pendingMemOnlyChunk;
    }
    const memOnlyChunk = this._memOnlyChunks.get(hash);
    if (memOnlyChunk !== undefined) {
      return memOnlyChunk;
    }
    // In order to delegate determining reachability to `computeRefCountUpdates`
    // and defer this determination until commit time, treat cache misses
    // as a 'put' of the lazily-loaded chunk.
    const pendingCachedChunk = this._pendingCachedChunks.get(hash);
    if (pendingCachedChunk !== undefined) {
      return pendingCachedChunk.chunk;
    }
    let chunk = this._sourceChunksCache.get(hash);
    if (chunk === undefined) {
      chunk = await (await this.getSourceRead()).getChunk(hash);
      if (chunk !== undefined) {
        this._pendingCachedChunks.set(chunk.hash, {chunk, size: -1});
      }
    }
    return chunk;
  }

  override getHead(name: string): Promise<Hash | undefined> {
    const headChange = this._pendingHeadChanges.get(name);
    if (headChange) {
      return Promise.resolve(headChange.new);
    }
    return super.getHead(name);
  }

  async commit(): Promise<void> {
    const pendingChunks = new Set(
      joinIterables(
        this._pendingMemOnlyChunks.keys(),
        this._pendingCachedChunks.keys(),
      ),
    );
    const refCountUpdates = await computeRefCountUpdates(
      this._pendingHeadChanges.values(),
      pendingChunks,
      this,
    );

    for (const [hash, count] of refCountUpdates) {
      if (this.isMemOnlyChunkHash(hash)) {
        if (count === 0) {
          this._refCounts.delete(hash);
          this._memOnlyChunks.delete(hash);
          this._refs.delete(hash);
        } else {
          this._refCounts.set(hash, count);
          const chunk = this._pendingMemOnlyChunks.get(hash);
          if (chunk) {
            this._refs.set(hash, chunk.meta);
            this._memOnlyChunks.set(hash, chunk);
          }
        }
        refCountUpdates.delete(hash);
      }
    }

    this._sourceChunksCache.updateForCommit(
      this._pendingCachedChunks,
      refCountUpdates,
    );

    for (const [name, headChange] of this._pendingHeadChanges) {
      if (headChange.new) {
        this._heads.set(name, headChange.new);
      } else {
        this._heads.delete(name);
      }
    }

    this._pendingMemOnlyChunks.clear();
    this._pendingCachedChunks.clear();
    this._pendingHeadChanges.clear();
    this.release();
  }

  getRefCount(hash: Hash): number | undefined {
    return this._refCounts.get(hash);
  }

  getRefs(hash: Hash): readonly Hash[] | undefined {
    const pendingMemOnlyChunk = this._pendingMemOnlyChunks.get(hash);
    if (pendingMemOnlyChunk) {
      return pendingMemOnlyChunk.meta;
    }
    const memOnlyChunk = this._memOnlyChunks.get(hash);
    if (memOnlyChunk) {
      return memOnlyChunk.meta;
    }
    const pendingCachedChunk = this._pendingCachedChunks.get(hash);
    if (pendingCachedChunk !== undefined) {
      return pendingCachedChunk.chunk.meta;
    }
    return this._refs.get(hash);
  }

  areRefsCounted(hash: Hash): boolean {
    return this._refs.has(hash);
  }

  chunksPersisted(chunkHashes: readonly Hash[]): void {
    const chunksToCache = [];
    for (const chunkHash of chunkHashes) {
      const chunk = this._memOnlyChunks.get(chunkHash);
      if (chunk) {
        this._memOnlyChunks.delete(chunkHash);
        chunksToCache.push(chunk);
      }
    }
    this._sourceChunksCache.persisted(chunksToCache);
  }
}

type CacheEntry = {
  chunk: Chunk;
  size: number;
};

class ChunksCache {
  private readonly _cacheSizeLimit: number;
  private readonly _getSizeOfChunk: (chunk: Chunk) => number;
  private readonly _refCounts: Map<Hash, number>;
  private readonly _refs: Map<Hash, readonly Hash[]>;
  private _size = 0;
  private _evictsAndDeletesSuspended = false;
  private readonly _suspendedDeletes: Hash[] = [];

  /**
   * Iteration order is from least to most recently used.
   *
   * Public so that testing subclass can access.
   */
  readonly cacheEntries = new Map<Hash, CacheEntry>();

  constructor(
    cacheSizeLimit: number,
    getSizeOfChunk: (v: Chunk) => number,
    refCounts: Map<Hash, number>,
    refs: Map<Hash, readonly Hash[]>,
  ) {
    this._cacheSizeLimit = cacheSizeLimit;
    this._getSizeOfChunk = getSizeOfChunk;
    this._refCounts = refCounts;
    this._refs = refs;
  }

  get(hash: Hash): Chunk | undefined {
    const cacheEntry = this.cacheEntries.get(hash);
    if (cacheEntry) {
      // Update order in map for LRU tracking.
      this.cacheEntries.delete(hash);
      this.cacheEntries.set(hash, cacheEntry);
    }
    return cacheEntry?.chunk;
  }

  getWithoutUpdatingLRU(hash: Hash): Chunk | undefined {
    return this.cacheEntries.get(hash)?.chunk;
  }

  put(chunk: Chunk): void {
    const {hash} = chunk;
    // If there is an existing cache entry then the cached value must be
    // equivalent.  Update order in map for LRU tracking and early return.
    const oldCacheEntry = this.cacheEntries.get(hash);
    if (oldCacheEntry) {
      this.cacheEntries.delete(hash);
      this.cacheEntries.set(hash, oldCacheEntry);
      return;
    }

    // Only cache if there is a ref from a head to this chunk
    const refCount = this._refCounts.get(hash);
    if (refCount === undefined || refCount < 1) {
      return;
    }
    if (!this._cacheChunk(chunk)) {
      return;
    }
    if (!this._refs.has(hash)) {
      for (const refHash of chunk.meta) {
        this._refCounts.set(refHash, (this._refCounts.get(refHash) || 0) + 1);
      }
      this._refs.set(hash, chunk.meta);
    }

    this._ensureCacheSizeLimit();
  }

  private _ensureCacheSizeLimit() {
    if (this._evictsAndDeletesSuspended) {
      return;
    }
    for (const entry of this.cacheEntries.values()) {
      if (this._size <= this._cacheSizeLimit) {
        break;
      }
      this._evict(entry);
    }
  }

  private _cacheChunk(chunk: Chunk, size?: number): boolean {
    const chunkSize = size ?? this._getSizeOfChunk(chunk);
    if (chunkSize > this._cacheSizeLimit) {
      // This value cannot be cached due to its size exceeding the
      // cache size limit, don't evict other entries to try to make
      // room for it.
      return false;
    }
    this._size += chunkSize;
    this.cacheEntries.set(chunk.hash, {chunk, size: chunkSize});
    return true;
  }

  private _evict(cacheEntry: CacheEntry): void {
    const {hash} = cacheEntry.chunk;
    this._size -= cacheEntry.size;
    this.cacheEntries.delete(hash);
  }

  private _delete(hash: Hash): void {
    this._refCounts.delete(hash);
    this._refs.delete(hash);
    const cacheEntry = this.cacheEntries.get(hash);
    if (cacheEntry) {
      this._size -= cacheEntry.size;
      this.cacheEntries.delete(hash);
    }
  }

  updateForCommit(
    chunksToPut: Map<Hash, {chunk: Chunk; size: number}>,
    refCountUpdates: Map<Hash, number>,
  ): void {
    for (const [hash, count] of refCountUpdates) {
      if (count === 0) {
        if (!this._evictsAndDeletesSuspended) {
          this._delete(hash);
        } else {
          this._refCounts.set(hash, 0);
          this._suspendedDeletes.push(hash);
        }
      } else {
        this._refCounts.set(hash, count);
        const chunkAndSize = chunksToPut.get(hash);
        if (chunkAndSize) {
          const {chunk, size} = chunkAndSize;
          const oldCacheEntry = this.cacheEntries.get(hash);
          if (oldCacheEntry) {
            // If there is an existing cache entry then the cached value must be
            // equivalent.  Update order in map for LRU tracking but avoid
            // recomputing size and creating a new cache entry.
            this.cacheEntries.delete(hash);
            this.cacheEntries.set(hash, oldCacheEntry);
          } else {
            this._cacheChunk(chunk, size !== -1 ? size : undefined);
            this._refs.set(hash, chunk.meta);
          }
        }
      }
    }
    this._ensureCacheSizeLimit();
  }

  persisted(chunks: Iterable<Chunk>) {
    for (const chunk of chunks) {
      this._cacheChunk(chunk);
    }
    this._ensureCacheSizeLimit();
  }

  async withSuspendedEvictsAndDeletes<T>(
    fn: () => MaybePromise<T>,
  ): Promise<T> {
    this._evictsAndDeletesSuspended = true;
    try {
      return await fn();
    } finally {
      this._evictsAndDeletesSuspended = false;
      for (const hash of this._suspendedDeletes) {
        if (this._refCounts.get(hash) === 0) {
          this._delete(hash);
        }
      }
      this._ensureCacheSizeLimit();
    }
  }
}
