import type * as dag from '../dag/mod.js';
import {Visitor} from '../dag/visitor.js';
import type {Hash} from '../hash.js';
import {promiseVoid} from '../resolved-promises.js';

export class GatherMemoryOnlyVisitor extends Visitor {
  readonly #gatheredChunks: Map<Hash, dag.Chunk> = new Map();
  readonly #lazyRead: dag.LazyRead;

  constructor(dagRead: dag.LazyRead) {
    super(dagRead);
    this.#lazyRead = dagRead;
  }

  get gatheredChunks(): ReadonlyMap<Hash, dag.Chunk> {
    return this.#gatheredChunks;
  }

  override visit(h: Hash): Promise<void> {
    if (!this.#lazyRead.isMemOnlyChunkHash(h)) {
      // Not a memory-only hash, no need to visit anything else.
      return promiseVoid;
    }
    return super.visit(h);
  }

  override visitChunk(chunk: dag.Chunk): Promise<void> {
    this.#gatheredChunks.set(chunk.hash, chunk);
    return super.visitChunk(chunk);
  }
}
