import type * as dag from '../dag/mod.js';
import {Visitor} from '../dag/visitor.js';
import type {Hash} from '../hash.js';
import {promiseVoid} from '../resolved-promises.js';
import {getSizeOfValue} from '../size-of-value.js';

export type ChunkWithSize = {chunk: dag.Chunk; size: number};

export class GatherNotCachedVisitor extends Visitor {
  readonly #gatheredChunks: Map<Hash, ChunkWithSize> = new Map();
  #gatheredChunksTotalSize = 0;
  readonly #lazyStore: dag.LazyStore;
  readonly #gatherSizeLimit: number;
  readonly #getSizeOfChunk: (chunk: dag.Chunk) => number;

  constructor(
    dagRead: dag.Read,
    lazyStore: dag.LazyStore,
    gatherSizeLimit: number,
    getSizeOfChunk: (chunk: dag.Chunk) => number = getSizeOfValue,
  ) {
    super(dagRead);
    this.#lazyStore = lazyStore;
    this.#gatherSizeLimit = gatherSizeLimit;
    this.#getSizeOfChunk = getSizeOfChunk;
  }

  get gatheredChunks(): ReadonlyMap<Hash, ChunkWithSize> {
    return this.#gatheredChunks;
  }

  override visit(h: Hash): Promise<void> {
    if (
      this.#gatheredChunksTotalSize >= this.#gatherSizeLimit ||
      this.#lazyStore.isCached(h)
    ) {
      return promiseVoid;
    }
    return super.visit(h);
  }

  override visitChunk(chunk: dag.Chunk): Promise<void> {
    if (this.#gatheredChunksTotalSize < this.#gatherSizeLimit) {
      const size = this.#getSizeOfChunk(chunk);
      this.#gatheredChunks.set(chunk.hash, {chunk, size});
      this.#gatheredChunksTotalSize += size;
    }

    return super.visitChunk(chunk);
  }
}
