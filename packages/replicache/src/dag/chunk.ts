import {assert} from 'shared/asserts.js';
import {Hash, newUUIDHash} from '../hash.js';
import {assertDeepFrozen} from '../json.js';

export type Refs = ReadonlySet<Hash>;

export const emptyRefs: Refs = new Set();

export class Chunk<V = unknown> {
  readonly hash: Hash;
  readonly data: V;

  /**
   * Meta is an array of refs. If there are no refs we do not write a meta
   * chunk.
   */
  readonly refs: Refs;

  constructor(hash: Hash, data: V, refs: Refs) {
    assert(!refs.has(hash), 'Chunk cannot reference itself');
    assertDeepFrozen(data);
    this.hash = hash;
    this.data = data;
    this.refs = refs;
  }
}

export function createChunk<V>(
  data: V,
  refs: Refs,
  chunkHasher: ChunkHasher,
): Chunk<V> {
  const hash = chunkHasher();
  return new Chunk(hash, data, refs);
}

export type CreateChunk = <V>(data: V, refs: Refs) => Chunk<V>;

export type ChunkHasher = () => Hash;

export {newUUIDHash as uuidChunkHasher};

export function throwChunkHasher(): Hash {
  throw new Error('unexpected call to compute chunk hash');
}
