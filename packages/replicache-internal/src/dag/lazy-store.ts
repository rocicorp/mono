import {RWLock} from '@rocicorp/lock';
import type {Hash} from '../hash.js';
import {Chunk, ChunkHasher, createChunk} from './chunk.js';
import {Store, Read, Write, mustGetChunk} from './store.js';
import {getSizeOfValue} from '../json.js';
import {
  computeRefCountUpdates,
  GarbageCollectionDelegate,
  HeadChange,
} from './gc.js';
import {assert, assertNotUndefined} from '../asserts.js';
import {promiseVoid} from '../resolved-promises.js';

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
   * and must have a write lock to
   * - write `_heads`
   * - write `_memOnlyChunks`
   */
  private readonly _rwLock = new RWLock();
  private readonly _heads = new Map<string, Hash>();
  private readonly _memOnlyChunks = new Map<Hash, Chunk>();
  private readonly _sourceChunksCache: ChunksCache;
  private readonly _sourceStore: Store;
  /**
   * These ref counts are independent from `this._sourceStore`'s ref counts.
   * These ref counts are based on reachability from `this._heads`.
   * The ref count for a hash is the number of unique heads or chunks in
   * `this._memOnlyChunks` or `this._sourceChunksCache` that reference this
   * hash.  That is, a chunk with a positive ref count is reachable from a head
   * via traversing chunk refs of chunks in `this._memOnlyChunks` or
   * `this._sourceChunksCache` .
   *
   * Invariant: all chunks in `this._memOnlyChunks` or `this._sourceChunksCache`
   * have a positive ref count in `this._refCounts`.
   *
   * A hash's ref count can be changed in two ways:
   * 1. A write commit updates a head (which can result in increasing or
   * decreasing ref count) or puts a chunk that references this hash (increasing
   * ref count).
   * 2. A chunk which references the hash is added to (increasing ref count) or
   * evicted from (decreasing ref count) this `this._sourceChunksCache`.
   *
   * Note: A chunk's hash may have an entry in `this._refCounts` without that
   * chunk being in `this._memOnlyChunks` or `this._sourceChunksCache`.  This is
   * the case when a head or chunk in `this._memOnlyChunks` or
   * `this._sourceChunksCache` references a chunk which is not currently cached
   * (either because it has not been read, or because it has been evicted).
   */
  private readonly _refCounts = new Map<Hash, number>();
  private readonly _chunkHasher: ChunkHasher;
  private readonly _assertValidHash: (hash: Hash) => void;

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

  async withRead<R>(fn: (read: LazyRead) => R | Promise<R>): Promise<R> {
    const read = await this.read();
    try {
      return await fn(read);
    } finally {
      read.close();
    }
  }

  async write(): Promise<LazyWrite> {
    const release = await this._rwLock.write();
    return new LazyWrite(
      this._heads,
      this._memOnlyChunks,
      this._sourceChunksCache,
      this._sourceStore,
      this._refCounts,
      release,
      this._chunkHasher,
      this._assertValidHash,
    );
  }

  async withWrite<R>(fn: (write: LazyWrite) => R | Promise<R>): Promise<R> {
    const write = await this.write();
    try {
      return await fn(write);
    } finally {
      write.close();
    }
  }

  close(): Promise<void> {
    return promiseVoid;
  }

  getRefCountsSnapshot(): ReadonlyMap<Hash, number> {
    return new Map(this._refCounts);
  }

  getMemOnlyChunksSnapshot(): ReadonlyMap<Hash, Chunk> {
    return new Map(this._memOnlyChunks);
  }

  /**
   * Does not acquire any lock on the store.
   */
  isCached(chunkHash: Hash): boolean {
    return (
      this._sourceChunksCache.getWithoutUpdatingLRU(chunkHash) !== undefined
    );
  }

  /**
   * Acquires a write lock on the store.
   */
  async chunksPersisted(chunkHashes: Iterable<Hash>): Promise<void> {
    await this.withWrite(() => {
      for (const chunkHash of chunkHashes) {
        const chunk = this._memOnlyChunks.get(chunkHash);
        if (chunk) {
          this._memOnlyChunks.delete(chunkHash);
          this._sourceChunksCache.put(chunk);
        }
      }
    });
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

  close(): void {
    if (!this._closed) {
      this._release();
      this._sourceRead
        ?.then(read => read.close())
        // If creation of the read failed there is nothing to close.
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
  implements Write, GarbageCollectionDelegate
{
  private readonly _refCounts: Map<Hash, number>;
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
    if (this._createdChunks.has(hash) || this.isMemOnlyChunkHash(c.hash)) {
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
    const refCountUpdates = await computeRefCountUpdates(
      this._pendingHeadChanges.values(),
      new Set([
        ...this._pendingMemOnlyChunks.keys(),
        ...this._pendingCachedChunks.keys(),
      ]),
      this,
    );

    const cacheHashesToDelete: Array<Hash> = [];
    for (const [hash, count] of refCountUpdates) {
      if (count === 0) {
        this._refCounts.delete(hash);
        this._pendingMemOnlyChunks.delete(hash);
        this._pendingCachedChunks.delete(hash);
        this._memOnlyChunks.delete(hash);
        cacheHashesToDelete.push(hash);
      } else {
        this._refCounts.set(hash, count);
      }
    }

    for (const [hash, chunk] of this._pendingMemOnlyChunks) {
      this._memOnlyChunks.set(hash, chunk);
    }

    for (const [name, headChange] of this._pendingHeadChanges) {
      if (headChange.new) {
        this._heads.set(name, headChange.new);
      } else {
        this._heads.delete(name);
      }
    }

    this._sourceChunksCache.updateForCommit(
      this._pendingCachedChunks.values(),
      cacheHashesToDelete,
    );
    this._pendingMemOnlyChunks.clear();
    this._pendingHeadChanges.clear();
    this.close();
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
    return this._sourceChunksCache.getWithoutUpdatingLRU(hash)?.meta;
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
  /**
   * Iteration order is from least to most recently used.
   */
  private readonly _cacheEntries = new Map<Hash, CacheEntry>();
  private _size = 0;

  constructor(
    cacheSizeLimit: number,
    getSizeOfChunk: (v: Chunk) => number,
    refCounts: Map<Hash, number>,
  ) {
    this._cacheSizeLimit = cacheSizeLimit;
    this._getSizeOfChunk = getSizeOfChunk;
    this._refCounts = refCounts;
  }

  get(hash: Hash): Chunk | undefined {
    const cacheEntry = this._cacheEntries.get(hash);
    if (cacheEntry) {
      // Update order in map for LRU tracking.
      this._cacheEntries.delete(hash);
      this._cacheEntries.set(hash, cacheEntry);
    }
    return cacheEntry?.chunk;
  }

  getWithoutUpdatingLRU(hash: Hash): Chunk | undefined {
    return this._cacheEntries.get(hash)?.chunk;
  }

  put(chunk: Chunk): void {
    const {hash} = chunk;
    // If there is an existing cache entry then the cached value must be
    // equivalent.  Update order in map for LRU tracking and early return.
    const oldCacheEntry = this._cacheEntries.get(hash);
    if (oldCacheEntry) {
      this._cacheEntries.delete(hash);
      this._cacheEntries.set(hash, oldCacheEntry);
      return;
    }

    // Only cache if there is a ref from a head to this chunk
    const refCount = this._refCounts.get(hash);
    if (refCount === undefined || refCount < 1) {
      return;
    }
    const valueSize = this._getSizeOfChunk(chunk);
    if (valueSize > this._cacheSizeLimit) {
      // This value cannot be cached due to its size exceeding the
      // cache size limit, don't evict other entries to try to make
      // room for it.
      return;
    }
    this._size += valueSize;
    this._cacheEntries.set(hash, {chunk, size: valueSize});
    for (const refHash of chunk.meta) {
      this._refCounts.set(refHash, (this._refCounts.get(refHash) || 0) + 1);
    }

    this._ensureCacheSizeLimit();
  }

  private _ensureCacheSizeLimit() {
    if (this._size <= this._cacheSizeLimit) {
      return;
    }
    for (const entry of this._cacheEntries) {
      if (this._size <= this._cacheSizeLimit) {
        break;
      }
      this.delete(entry[1]);
    }
  }

  delete(cacheEntry: CacheEntry): void {
    const {hash} = cacheEntry.chunk;
    this._size -= cacheEntry.size;
    this._cacheEntries.delete(hash);
    for (const refHash of cacheEntry.chunk.meta) {
      const oldCount = this._refCounts.get(refHash);
      assertNotUndefined(oldCount);
      assert(oldCount > 0);
      const newCount = oldCount - 1;
      if (newCount === 0) {
        this._refCounts.delete(refHash);
        const refCacheEntry = this._cacheEntries.get(refHash);
        if (refCacheEntry) {
          assert(
            cacheEntry.chunk.hash !== refHash,
            'Found a chunk that references itself',
          );
          this.delete(refCacheEntry);
        }
      } else {
        this._refCounts.set(refHash, newCount);
      }
    }
  }

  updateForCommit(
    chunksToPut: Iterable<{chunk: Chunk; size: number}>,
    hashesToDelete: Iterable<Hash>,
  ): void {
    // Commit has already updated this._refCounts to reflect these puts and
    // deletes.  First we do all the puts and deletes to bring this._refCounts
    // and this._cacheEntries into a consistent state. Then we ensure that the
    // cache is below its size limit, using this.delete and
    // this._ensureCacheSizeLimit, both of which require this._refCounts and
    // this._cacheEntries to be in a consistent state to work correctly.
    const cacheEntriesLargerThanLimit = [];
    for (const {chunk, size} of chunksToPut) {
      const {hash} = chunk;
      const oldCacheEntry = this._cacheEntries.get(hash);
      if (oldCacheEntry) {
        // If there is an existing cache entry then the cached value must be
        // equivalent.  Update order in map for LRU tracking but avoid
        // recomputing size and creating a new cache entry.
        this._cacheEntries.delete(hash);
        this._cacheEntries.set(hash, oldCacheEntry);
      } else {
        const valueSize = size !== -1 ? size : this._getSizeOfChunk(chunk);
        this._size += valueSize;
        const cacheEntry = {chunk, size: valueSize};
        this._cacheEntries.set(hash, cacheEntry);
        if (valueSize > this._cacheSizeLimit) {
          cacheEntriesLargerThanLimit.push(cacheEntry);
        }
      }
    }

    for (const hash of hashesToDelete) {
      const cacheEntryToDelete = this._cacheEntries.get(hash);
      if (cacheEntryToDelete) {
        this._size -= cacheEntryToDelete.size;
        this._cacheEntries.delete(hash);
      }
    }

    // First delete any put value that cannot be cached due to its size
    // exceeding the cache size limit.  This avoids this._ensureCacheSizeLimit
    // evicting entries trying to make room for these values which should not be
    // cached.
    for (const cacheEntry of cacheEntriesLargerThanLimit) {
      this.delete(cacheEntry);
    }

    this._ensureCacheSizeLimit();
  }
}
