import type * as dag from '../dag/mod.js';
import {Visitor} from '../dag/visitor.js';
import type {Hash} from '../hash.js';
import {promiseVoid} from '../resolved-promises.js';

export class GatherMemoryOnlyVisitor extends Visitor {
  private readonly _gatheredChunks: Map<Hash, dag.Chunk> = new Map();
  private readonly _lazyRead: dag.LazyRead;

  constructor(dagRead: dag.LazyRead) {
    super(dagRead);
    this._lazyRead = dagRead;
  }

  get gatheredChunks(): ReadonlyMap<Hash, dag.Chunk> {
    return this._gatheredChunks;
  }

  override visit(h: Hash): Promise<void> {
    if (!this._lazyRead.isMemOnlyChunkHash(h)) {
      // Not a memory-only hash, no need to visit anything else.
      return promiseVoid;
    }
    return super.visit(h);
  }

  override visitChunk(chunk: dag.Chunk): Promise<void> {
    this._gatheredChunks.set(chunk.hash, chunk);
    return super.visitChunk(chunk);
  }
}
