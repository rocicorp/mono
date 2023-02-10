import {assert, assertString} from '../asserts';
import {Hash, makeNewFakeHashFunction, hashOf} from '../hash';
import type {Value} from '../kv/store';

type Refs = readonly Hash[];

export interface Chunk<V extends Value = Value> {
  readonly hash: Hash;
  readonly data: V;
  /**
   * Meta is an array of refs. If there are no refs we do not write a meta
   * chunk.
   */
  readonly meta: Refs;
}

class ChunkImpl<V extends Value = Value> implements Chunk<V> {
  readonly hash: Hash;
  readonly data: V;
  readonly meta: Refs;

  constructor(hash: Hash, data: V, meta: Refs) {
    this.hash = hash;
    this.data = data;
    this.meta = meta;
  }
}

export function assertMeta(v: unknown): asserts v is Refs {
  if (!Array.isArray(v)) {
    throw new Error('Meta must be an array');
  }
  for (const e of v) {
    assertString(e);
  }
}

export function createChunk<V extends Value>(
  data: V,
  refs: Refs,
  chunkHasher: ChunkHasher,
): Chunk<V> {
  const hash = chunkHasher(data);
  return createChunkWithHash(hash, data, refs);
}

export function createChunkWithHash<V extends Value>(
  hash: Hash,
  data: V,
  refs: Refs,
): Chunk<V> {
  assert(!refs.includes(hash), 'Chunk cannot reference itself');
  return new ChunkImpl(hash, data, refs);
}

export async function createChunkWithNativeHash<V extends Value>(
  data: V,
  refs: Refs,
): Promise<Chunk<V>> {
  const hash = await hashOf(data);
  return createChunkWithHash(hash, data, refs);
}

export type CreateChunk = <V extends Value>(data: V, refs: Refs) => Chunk<V>;

export type ChunkHasher = (data: Value) => Hash;

export function makeTestChunkHasher(prefix = 'fake'): ChunkHasher {
  const makeHash = makeNewFakeHashFunction(prefix);
  const map = new Map<string, Hash>();
  const ch: ChunkHasher = data => {
    const jsonString = JSON.stringify(data);
    const h = map.get(jsonString);
    if (h) {
      return h;
    }
    const h2 = makeHash();
    map.set(jsonString, h2);
    return h2;
  };
  return ch;
}

export function throwChunkHasher(_data: Value): Hash {
  throw new Error('unexpected call to compute chunk hash');
}
