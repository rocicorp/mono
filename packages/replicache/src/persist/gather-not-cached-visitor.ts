import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import type {Chunk} from '../dag/chunk.ts';
import type {LazyStore} from '../dag/lazy-store.ts';
import type {Read} from '../dag/store.ts';
import {Visitor} from '../dag/visitor.ts';
import type {Hash} from '../hash.ts';
import {getSizeOfValue} from '../size-of-value.ts';

export type ChunkWithSize = {chunk: Chunk; size: number};

export class GatherNotCachedVisitor extends Visitor {
  readonly #gatheredChunks: Map<Hash, ChunkWithSize> = new Map();
  #gatheredChunksTotalSize = 0;
  readonly #lazyStore: LazyStore;
  readonly #gatherSizeLimit: number;
  readonly #getSizeOfChunk: (chunk: Chunk) => number;

  constructor(
    dagRead: Read,
    lazyStore: LazyStore,
    gatherSizeLimit: number,
    getSizeOfChunk: (chunk: Chunk) => number = getSizeOfValue,
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

  override visitChunk(chunk: Chunk): Promise<void> {
    if (this.#gatheredChunksTotalSize < this.#gatherSizeLimit) {
      const size = this.#getSizeOfChunk(chunk);
      this.#gatheredChunks.set(chunk.hash, {chunk, size});
      this.#gatheredChunksTotalSize += size;
    }

    return super.visitChunk(chunk);
  }
}
