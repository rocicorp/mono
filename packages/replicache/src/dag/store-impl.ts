import {assertNumber} from 'shared/src/asserts.js';
import {Hash, assertHash} from '../hash.js';
import type {ReadonlyJSONValue} from '../json.js';
import type * as kv from '../kv/mod.js';
import {Chunk, ChunkHasher, assertMeta, createChunk} from './chunk.js';
import {RefCountUpdatesDelegate, computeRefCountUpdates} from './gc.js';
import {chunkDataKey, chunkMetaKey, chunkRefCountKey, headKey} from './key.js';
import {Read, Store, Write, mustGetChunk} from './store.js';

export class StoreImpl implements Store {
  private readonly _kv: kv.Store;
  private readonly _chunkHasher: ChunkHasher;
  private readonly _assertValidHash: (hash: Hash) => void;

  constructor(
    kv: kv.Store,
    chunkHasher: ChunkHasher,
    assertValidHash: (hash: Hash) => void,
  ) {
    this._kv = kv;
    this._chunkHasher = chunkHasher;
    this._assertValidHash = assertValidHash;
  }

  async read(): Promise<Read> {
    return new ReadImpl(await this._kv.read(), this._assertValidHash);
  }

  async write(): Promise<Write> {
    return new WriteImpl(
      await this._kv.write(),
      this._chunkHasher,
      this._assertValidHash,
    );
  }

  close(): Promise<void> {
    return this._kv.close();
  }
}

export class ReadImpl implements Read {
  protected readonly _tx: kv.Read;
  readonly assertValidHash: (hash: Hash) => void;

  constructor(kv: kv.Read, assertValidHash: (hash: Hash) => void) {
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
    let refs: readonly Hash[];
    if (refsVal !== undefined) {
      assertMeta(refsVal);
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
  protected declare readonly _tx: kv.Write;
  private readonly _chunkHasher: ChunkHasher;

  private readonly _putChunks = new Set<Hash>();
  private readonly _changedHeads = new Map<string, HeadChange>();

  constructor(
    kvw: kv.Write,
    chunkHasher: ChunkHasher,
    assertValidHash: (hash: Hash) => void,
  ) {
    super(kvw, assertValidHash);
    this._chunkHasher = chunkHasher;
  }

  createChunk = <V>(data: V, refs: readonly Hash[]): Chunk<V> =>
    createChunk(data, refs, this._chunkHasher);

  get kvWrite(): kv.Write {
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
    this._putChunks.add(hash);
    await p1;
    await p2;
  }

  setHead(name: string, hash: Hash): Promise<void> {
    return this._setHead(name, hash);
  }

  removeHead(name: string): Promise<void> {
    return this._setHead(name, undefined);
  }

  private async _setHead(name: string, hash: Hash | undefined): Promise<void> {
    const oldHash = await this.getHead(name);
    const hk = headKey(name);

    let p1: Promise<void>;
    if (hash === undefined) {
      p1 = this._tx.del(hk);
    } else {
      p1 = this._tx.put(hk, hash);
    }

    const v = this._changedHeads.get(name);
    if (v === undefined) {
      this._changedHeads.set(name, {new: hash, old: oldHash});
    } else {
      // Keep old if existing
      v.new = hash;
    }

    await p1;
  }

  async commit(): Promise<void> {
    const refCountUpdates = await computeRefCountUpdates(
      this._changedHeads.values(),
      this._putChunks,
      this,
    );
    await this._applyRefCountUpdates(refCountUpdates);
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
    assertMeta(meta);
    return meta;
  }

  private async _applyRefCountUpdates(
    refCountCache: Map<Hash, number>,
  ): Promise<void> {
    const ps: Promise<void>[] = [];
    for (const [hash, count] of refCountCache) {
      if (count === 0) {
        ps.push(this._removeAllRelatedKeys(hash));
      } else {
        const refCountKey = chunkRefCountKey(hash);
        ps.push(this._tx.put(refCountKey, count));
      }
    }
    await Promise.all(ps);
  }

  private async _removeAllRelatedKeys(hash: Hash): Promise<void> {
    await Promise.all([
      this._tx.del(chunkDataKey(hash)),
      this._tx.del(chunkMetaKey(hash)),
      this._tx.del(chunkRefCountKey(hash)),
    ]);

    this._putChunks.delete(hash);
  }

  release(): void {
    this._tx.release();
  }
}
