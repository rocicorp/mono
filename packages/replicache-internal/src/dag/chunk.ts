import {assert, assertString} from '../asserts';
import {Hash, newUUIDHash} from '../hash';
import type {ReadonlyJSONValue} from '../json.js';

type Refs = readonly Hash[];

export interface Chunk<V = unknown> {
  readonly hash: Hash;
  readonly data: V;
  /**
   * Meta is an array of refs. If there are no refs we do not write a meta
   * chunk.
   */
  readonly meta: Refs;
}

class ChunkImpl<V = ReadonlyJSONValue> implements Chunk<V> {
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

export function createChunk<V>(
  data: V,
  refs: Refs,
  chunkHasher: ChunkHasher,
): Chunk<V> {
  const hash = chunkHasher();
  return createChunkWithHash(hash, data, refs);
}

export function createChunkWithHash<V>(
  hash: Hash,
  data: V,
  refs: Refs,
): Chunk<V> {
  assert(!refs.includes(hash), 'Chunk cannot reference itself');
  return new ChunkImpl(hash, data, refs);
}

export type CreateChunk = <V>(data: V, refs: Refs) => Chunk<V>;

export type ChunkHasher = () => Hash;

export {newUUIDHash as uuidChunkHasher};

export function throwChunkHasher(): Hash {
  throw new Error('unexpected call to compute chunk hash');
}
