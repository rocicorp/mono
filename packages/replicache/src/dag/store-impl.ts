import {assertNumber} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {type Hash, assertHash} from '../hash.ts';
import type {
  Read as KVRead,
  Store as KVStore,
  Write as KVWrite,
} from '../kv/store.ts';
import {
  Chunk,
  type ChunkHasher,
  type Refs,
  assertRefs,
  createChunk,
} from './chunk.ts';
import {type RefCountUpdatesDelegate, computeRefCountUpdates} from './gc.ts';
import {chunkDataKey, chunkMetaKey, chunkRefCountKey, headKey} from './key.ts';
import {type Read, type Store, type Write, mustGetChunk} from './store.ts';

export class StoreImpl implements Store {
  readonly #kv: KVStore;
  readonly #chunkHasher: ChunkHasher;
  readonly #assertValidHash: (hash: Hash) => void;

  constructor(
    kv: KVStore,
    chunkHasher: ChunkHasher,
    assertValidHash: (hash: Hash) => void,
  ) {
    this.#kv = kv;
    this.#chunkHasher = chunkHasher;
    this.#assertValidHash = assertValidHash;
  }

  async read(): Promise<Read> {
    return new ReadImpl(await this.#kv.read(), this.#assertValidHash);
  }

  async write(): Promise<Write> {
    return new WriteImpl(
      await this.#kv.write(),
      this.#chunkHasher,
      this.#assertValidHash,
    );
  }

  close(): Promise<void> {
    return this.#kv.close();
  }
}

export class ReadImpl implements Read {
  protected readonly _tx: KVRead;
  readonly assertValidHash: (hash: Hash) => void;

  constructor(kv: KVRead, assertValidHash: (hash: Hash) => void) {
    this._tx = kv;
    this.assertValidHash = assertValidHash;
  }

  hasChunk(hash: Hash): Promise<boolean> {
    return this._tx.has(chunkDataKey(hash));
  }

  async getChunk(hash: Hash): Promise<Chunk | undefined> {
    const data = await this._tx.get(chunkDataKey(hash));
    if (data === undefined) {
      return undefined;
    }

    const refsVal = await this._tx.get(chunkMetaKey(hash));
    let refs: Refs;
    if (refsVal !== undefined) {
      assertRefs(refsVal);
      refs = refsVal;
    } else {
      refs = [];
    }
    return new Chunk(hash, data, refs);
  }

  mustGetChunk(hash: Hash): Promise<Chunk> {
    return mustGetChunk(this, hash);
  }

  async getHead(name: string): Promise<Hash | undefined> {
    const data = await this._tx.get(headKey(name));
    if (data === undefined) {
      return undefined;
    }
    assertHash(data);
    return data;
  }

  release(): void {
    this._tx.release();
  }

  get closed(): boolean {
    return this._tx.closed;
  }
}

type HeadChange = {
  new: Hash | undefined;
  old: Hash | undefined;
};

export class WriteImpl
  extends ReadImpl
  implements Write, RefCountUpdatesDelegate
{
  protected declare readonly _tx: KVWrite;
  readonly #chunkHasher: ChunkHasher;

  readonly #putChunks = new Set<Hash>();
  readonly #changedHeads = new Map<string, HeadChange>();

  constructor(
    kvw: KVWrite,
    chunkHasher: ChunkHasher,
    assertValidHash: (hash: Hash) => void,
  ) {
    super(kvw, assertValidHash);
    this.#chunkHasher = chunkHasher;
  }

  createChunk = <V>(data: V, refs: Refs): Chunk<V> =>
    createChunk(data, refs, this.#chunkHasher);

  get kvWrite(): KVWrite {
    return this._tx;
  }

  async putChunk(c: Chunk): Promise<void> {
    const {hash, data, meta} = c;
    // We never want to write temp hashes to the underlying store.
    this.assertValidHash(hash);
    const key = chunkDataKey(hash);
    // Commit contains InternalValue and Hash which are opaque types.
    const p1 = this._tx.put(key, data as ReadonlyJSONValue);
    let p2;
    if (meta.length > 0) {
      for (const h of meta) {
        this.assertValidHash(h);
      }
      p2 = this._tx.put(chunkMetaKey(hash), meta);
    }
    this.#putChunks.add(hash);
    await p1;
    await p2;
  }

  setHead(name: string, hash: Hash): Promise<void> {
    return this.#setHead(name, hash);
  }

  removeHead(name: string): Promise<void> {
    return this.#setHead(name, undefined);
  }

  async #setHead(name: string, hash: Hash | undefined): Promise<void> {
    const oldHash = await this.getHead(name);
    const hk = headKey(name);

    let p1: Promise<void>;
    if (hash === undefined) {
      p1 = this._tx.del(hk);
    } else {
      p1 = this._tx.put(hk, hash);
    }

    const v = this.#changedHeads.get(name);
    if (v === undefined) {
      this.#changedHeads.set(name, {new: hash, old: oldHash});
    } else {
      // Keep old if existing
      v.new = hash;
    }

    await p1;
  }

  async commit(): Promise<void> {
    const refCountUpdates = await computeRefCountUpdates(
      this.#changedHeads.values(),
      this.#putChunks,
      this,
    );
    await this.#applyRefCountUpdates(refCountUpdates);
    await this._tx.commit();
  }

  async getRefCount(hash: Hash): Promise<number | undefined> {
    const value = await this._tx.get(chunkRefCountKey(hash));
    if (value === undefined) {
      return undefined;
    }
    assertNumber(value);
    if (value < 0 || value > 0xffff || value !== (value | 0)) {
      throw new Error(
        `Invalid ref count ${value}. We expect the value to be a Uint16`,
      );
    }
    return value;
  }

  async getRefs(hash: Hash): Promise<readonly Hash[]> {
    const meta = await this._tx.get(chunkMetaKey(hash));
    if (meta === undefined) {
      return [];
    }
    assertRefs(meta);
    return meta;
  }

  async #applyRefCountUpdates(refCountCache: Map<Hash, number>): Promise<void> {
    const ps: Promise<void>[] = [];
    for (const [hash, count] of refCountCache) {
      if (count === 0) {
        ps.push(this.#removeAllRelatedKeys(hash));
      } else {
        const refCountKey = chunkRefCountKey(hash);
        ps.push(this._tx.put(refCountKey, count));
      }
    }
    await Promise.all(ps);
  }

  async #removeAllRelatedKeys(hash: Hash): Promise<void> {
    await Promise.all([
      this._tx.del(chunkDataKey(hash)),
      this._tx.del(chunkMetaKey(hash)),
      this._tx.del(chunkRefCountKey(hash)),
    ]);

    this.#putChunks.delete(hash);
  }

  release(): void {
    this._tx.release();
  }
}
