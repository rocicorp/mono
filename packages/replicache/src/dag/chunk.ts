import {assert, assertString} from 'shared/src/asserts.js';
import {assertDeepFrozen} from '../frozen-json.js';
import type {Hash} from '../hash.js';

// By using declare we tell the type system that there is a unique symbol.
// However, there is no such symbol but the type system does not care.
declare const refsTag: unique symbol;

/**
 * Opaque type representing a Refs. The reason to use an opaque type here is to
 * make sure that Refs are always sorted and have no duplicates.
 */
export type Refs = [] | readonly [Hash] | (readonly Hash[] & {[refsTag]: true});

/**
 * Convert to a Refs when we already know it is sorted and has no duplicates.
 */
export function asRefs(sortedRefs: Hash[]): Refs {
  return sortedRefs as unknown as Refs;
}

/**
 * Sorts and tags as Refs. If an Array is passed in the array is sorted in
 * place, otherwise a copy of the iterable is created. This checks for duplicates.
 */
export function toRefs(refs: Hash[] | Set<Hash>): Refs {
  if (Array.isArray(refs)) {
    refs.sort();
    for (let i = 1; i < refs.length; i++) {
      assert(refs[i - 1] !== refs[i], 'Refs must not have duplicates');
    }
    return asRefs(refs);
  }

  const refsArray = [...refs];
  refsArray.sort();
  // no need to check for duplicates as Set cannot have duplicates.
  return asRefs(refsArray);
}

export class Chunk<V = unknown> {
  readonly hash: Hash;
  readonly data: V;

  /**
   * Meta is an array of refs. If there are no refs we do not write a meta
   * chunk.
   */
  readonly meta: Refs;

  constructor(hash: Hash, data: V, refs: Refs) {
    assert(
      !(refs as unknown[]).includes(hash),
      'Chunk cannot reference itself',
    );
    assertDeepFrozen(data);
    this.hash = hash;
    this.data = data;
    this.meta = refs;
  }
}

export function assertRefs(v: unknown): asserts v is Refs {
  if (!Array.isArray(v)) {
    throw new Error('Refs must be an array');
  }
  if (v.length > 0) {
    assertString(v[0]);
    for (let i = 1; i < v.length; i++) {
      assertString(v[i]);
      assert(v[i - 1] < v[i], 'Refs must be sorted and have no duplicates');
    }
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

export function throwChunkHasher(): Hash {
  throw new Error('unexpected call to compute chunk hash');
}
