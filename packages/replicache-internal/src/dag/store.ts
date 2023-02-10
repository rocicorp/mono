import type {Chunk} from './chunk.js';
import type {Hash} from '../hash.js';

export interface Store {
  read(): Promise<Read>;
  withRead<R>(fn: (read: Read) => R | Promise<R>): Promise<R>;
  write(): Promise<Write>;
  withWrite<R>(fn: (Write: Write) => R | Promise<R>): Promise<R>;
  close(): Promise<void>;
}

interface GetChunk {
  getChunk(hash: Hash): Promise<Chunk | undefined>;
}

export interface MustGetChunk {
  mustGetChunk(hash: Hash): Promise<Chunk>;
}

export interface Read extends GetChunk, MustGetChunk {
  hasChunk(hash: Hash): Promise<boolean>;
  getHead(name: string): Promise<Hash | undefined>;
  close(): void;
  get closed(): boolean;
}

export interface Write extends Read {
  createChunk<V>(data: V, refs: readonly Hash[]): Chunk<V>;
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
