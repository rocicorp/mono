import * as db from '../db/mod.js';
import type {Hash} from '../hash.js';
import type * as dag from '../dag/mod.js';
import type * as btree from '../btree/mod.js';
import type {HashRefType} from '../db/hash-ref-type.js';
import type {Meta} from '../db/commit.js';
import {promiseVoid} from '../resolved-promises.js';

export class GatherMemoryOnlyVisitor extends db.Visitor {
  private readonly _gatheredChunks: Map<Hash, dag.Chunk> = new Map();
  private readonly _lazyRead: dag.LazyRead;

  constructor(dagRead: dag.LazyRead) {
    super(dagRead);
    this._lazyRead = dagRead;
  }

  get gatheredChunks(): ReadonlyMap<Hash, dag.Chunk> {
    return this._gatheredChunks;
  }

  override visitCommit(h: Hash, hashRefType?: HashRefType): Promise<void> {
    if (!this._lazyRead.isMemOnlyChunkHash(h)) {
      // Not a memory-only hash, no need to visit anything else.
      return promiseVoid;
    }
    return super.visitCommit(h, hashRefType);
  }

  override visitCommitChunk(
    chunk: dag.Chunk<db.CommitData<Meta>>,
  ): Promise<void> {
    this._gatheredChunks.set(chunk.hash, chunk);
    return super.visitCommitChunk(chunk);
  }

  override visitBTreeNode(h: Hash): Promise<void> {
    if (!this._lazyRead.isMemOnlyChunkHash(h)) {
      // Not a memory-only hash, no need to visit anything else.
      return promiseVoid;
    }

    return super.visitBTreeNode(h);
  }

  override visitBTreeNodeChunk(chunk: dag.Chunk<btree.Node>): Promise<void> {
    this._gatheredChunks.set(chunk.hash, chunk);
    return super.visitBTreeNodeChunk(chunk);
  }
}
