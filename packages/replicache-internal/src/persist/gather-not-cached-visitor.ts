import * as db from '../db/mod.js';
import type {Hash} from '../hash.js';
import type * as dag from '../dag/mod.js';
import type * as btree from '../btree/mod.js';
import {getSizeOfValue} from '../json.js';
import {promiseVoid} from '../resolved-promises.js';

export type ChunkWithSize = {chunk: dag.Chunk; size: number};

export class GatherNotCachedVisitor extends db.Visitor {
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

  private _shouldVisit(h: Hash): boolean {
    return (
      this._gatheredChunksTotalSize < this._gatherSizeLimit &&
      !this._lazyStore.isCached(h)
    );
  }

  private _gather(chunk: dag.Chunk): void {
    const size = this._getSizeOfChunk(chunk);
    this._gatheredChunks.set(chunk.hash, {chunk, size});
    this._gatheredChunksTotalSize += size;
  }

  override visitCommit(h: Hash, hashRefType?: db.HashRefType): Promise<void> {
    if (!this._shouldVisit(h)) {
      return promiseVoid;
    }
    return super.visitCommit(h, hashRefType);
  }

  override visitCommitChunk(
    chunk: dag.Chunk<db.CommitData<db.Meta>>,
  ): Promise<void> {
    this._gather(chunk);
    return super.visitCommitChunk(chunk);
  }

  override visitBTreeNode(h: Hash): Promise<void> {
    if (!this._shouldVisit(h)) {
      return promiseVoid;
    }
    return super.visitBTreeNode(h);
  }

  override visitBTreeNodeChunk(chunk: dag.Chunk<btree.Node>): Promise<void> {
    this._gather(chunk);
    return super.visitBTreeNodeChunk(chunk);
  }
}
