import {assert} from '../../../shared/src/asserts.ts';
import type {Hash} from '../hash.ts';
import type {Release} from '../with-transactions.ts';
import type {Chunk, Refs} from './chunk.ts';

export interface Store {
  read(): Promise<Read>;
  write(): Promise<Write>;
  close(): Promise<void>;
}

interface GetChunk {
  getChunk(hash: Hash): Promise<Chunk | undefined>;
}

export interface MustGetChunk {
  mustGetChunk(hash: Hash): Promise<Chunk>;
}

export interface Read extends GetChunk, MustGetChunk, Release {
  hasChunk(hash: Hash): Promise<boolean>;
  getHead(name: string): Promise<Hash | undefined>;
  get closed(): boolean;
}

export interface Write extends Read {
  createChunk<V>(data: V, refs: Refs): Chunk<V>;
  putChunk<V>(c: Chunk<V>): Promise<void>;
  setHead(name: string, hash: Hash): Promise<void>;
  removeHead(name: string): Promise<void>;
  assertValidHash(hash: Hash): void;
  commit(): Promise<void>;
}

export class ChunkNotFoundError extends Error {
  name = 'ChunkNotFoundError';
  readonly hash: Hash;
  constructor(hash: Hash) {
    super(`Chunk not found ${hash}`);
    this.hash = hash;
  }
}

/**
 * Thrown when a ref count read from the store is not a valid Uint16. This
 * means the store is corrupt (due to some unknown bug) and cannot be repaired.
 */
export class InvalidRefCountError extends Error {
  name = 'InvalidRefCountError';
  readonly hash: Hash;
  readonly value: unknown;
  constructor(hash: Hash, value: unknown) {
    super(
      `Invalid ref count ${String(value)} for ${hash}. We expect the value to be a Uint16`,
    );
    this.hash = hash;
    this.value = value;
  }
}

export async function mustGetChunk(
  store: GetChunk,
  hash: Hash,
): Promise<Chunk> {
  const chunk = await store.getChunk(hash);
  if (chunk) {
    return chunk;
  }
  throw new ChunkNotFoundError(hash);
}

export async function mustGetHeadHash(
  name: string,
  store: Read,
): Promise<Hash> {
  const hash = await store.getHead(name);
  assert(hash, `Missing head ${name}`);
  return hash;
}
