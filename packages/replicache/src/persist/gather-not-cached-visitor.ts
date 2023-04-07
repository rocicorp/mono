import type * as dag from '../dag/mod.js';
import {Visitor} from '../dag/visitor.js';
import type {Hash} from '../hash.js';
import {promiseVoid} from '../resolved-promises.js';
import {getSizeOfValue} from '../size-of-value.js';

export type ChunkWithSize = {chunk: dag.Chunk; size: number};

export class GatherNotCachedVisitor extends Visitor {
  private readonly _gatheredChunks: Map<Hash, ChunkWithSize> = new Map();
  private _gatheredChunksTotalSize = 0;
  private readonly _lazyStore: dag.LazyStore;
  private readonly _gatherSizeLimit: number;
  private readonly _getSizeOfChunk: (chunk: dag.Chunk) => number;

  constructor(
    dagRead: dag.Read,
    lazyStore: dag.LazyStore,
    gatherSizeLimit: number,
    getSizeOfChunk: (chunk: dag.Chunk) => number = getSizeOfValue,
  ) {
    super(dagRead);
    this._lazyStore = lazyStore;
    this._gatherSizeLimit = gatherSizeLimit;
    this._getSizeOfChunk = getSizeOfChunk;
  }

  get gatheredChunks(): ReadonlyMap<Hash, ChunkWithSize> {
    return this._gatheredChunks;
  }

  override visit(h: Hash): Promise<void> {
    if (
      this._gatheredChunksTotalSize >= this._gatherSizeLimit ||
      this._lazyStore.isCached(h)
    ) {
      return promiseVoid;
    }
    return super.visit(h);
  }

  override visitChunk(chunk: dag.Chunk): Promise<void> {
    if (this._gatheredChunksTotalSize < this._gatherSizeLimit) {
      const size = this._getSizeOfChunk(chunk);
      this._gatheredChunks.set(chunk.hash, {chunk, size});
      this._gatheredChunksTotalSize += size;
    }

    return super.visitChunk(chunk);
  }
}
