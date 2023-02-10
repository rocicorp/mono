import {
  assertCommitData,
  CommitData,
  IndexChangeMeta,
  IndexRecord,
  LocalMetaSDD,
  Meta,
  MetaType,
  SnapshotMeta,
  SnapshotMetaDD31,
} from './commit';
import type * as dag from '../dag/mod';
import {emptyHash, Hash} from '../hash';
import {
  internalizeBTreeNode,
  InternalNode,
  isInternalNode,
  Node,
} from '../btree/node';
import {HashRefType} from './hash-ref-type';
import {ChunkNotFoundError} from '../dag/store.js';

export class Visitor {
  readonly dagRead: dag.Read;
  private _visitedHashes: Set<Hash> = new Set();

  constructor(dagRead: dag.Read) {
    this.dagRead = dagRead;
  }

  async visitCommit(
    h: Hash,
    hashRefType = HashRefType.RequireStrong,
  ): Promise<void> {
    if (this._visitedHashes.has(h)) {
      return;
    }
    this._visitedHashes.add(h);

    const chunk = await this.dagRead.getChunk(h);
    if (!chunk) {
      if (hashRefType === HashRefType.AllowWeak) {
        return;
      }
      throw new ChunkNotFoundError(h);
    }

    const {data} = chunk;
    assertCommitData(data);
    await this.visitCommitChunk(chunk as dag.Chunk<CommitData<Meta>>);
  }

  async visitCommitChunk(chunk: dag.Chunk<CommitData<Meta>>): Promise<void> {
    const {data} = chunk;
    await Promise.all([
      this._visitCommitMeta(data.meta),
      this._visitCommitValue(data.valueHash),
      this._visitCommitIndexes(data.indexes),
    ]);
  }

  private _visitCommitMeta(meta: Meta): Promise<void> {
    switch (meta.type) {
      case MetaType.IndexChange:
        return this._visitIndexChangeMeta(meta);

      case MetaType.Local:
        return this._visitLocalMeta(meta);

      case MetaType.Snapshot:
        return this._visitSnapshot(meta);
    }
  }

  private async _visitBasisHash(
    basisHash: Hash | null,
    hashRefType?: HashRefType,
  ): Promise<void> {
    if (basisHash !== null) {
      await this.visitCommit(basisHash, hashRefType);
    }
  }

  private async _visitSnapshot(
    meta: SnapshotMeta | SnapshotMetaDD31,
  ): Promise<void> {
    // basisHash is weak for Snapshot Commits
    await this._visitBasisHash(meta.basisHash, HashRefType.AllowWeak);
  }

  private async _visitLocalMeta(meta: LocalMetaSDD): Promise<void> {
    await this._visitBasisHash(meta.basisHash, HashRefType.RequireStrong);
    if (meta.originalHash !== null) {
      await this.visitCommit(meta.originalHash, HashRefType.AllowWeak);
    }
  }

  private _visitIndexChangeMeta(meta: IndexChangeMeta): Promise<void> {
    return this._visitBasisHash(meta.basisHash, HashRefType.RequireStrong);
  }

  private _visitCommitValue(valueHash: Hash): Promise<void> {
    return this.visitBTreeNode(valueHash);
  }

  async visitBTreeNode(h: Hash): Promise<void> {
    // we use the emptyHash for an empty btree
    if (h === emptyHash) {
      return;
    }

    if (this._visitedHashes.has(h)) {
      return;
    }
    this._visitedHashes.add(h);

    const chunk = await this.dagRead.mustGetChunk(h);
    const {data} = chunk;
    internalizeBTreeNode(data);
    await this.visitBTreeNodeChunk(chunk as dag.Chunk<Node>);
  }

  async visitBTreeNodeChunk(chunk: dag.Chunk<Node>): Promise<void> {
    const {data} = chunk;
    if (isInternalNode(data)) {
      await this._visitBTreeInternalNode(chunk as dag.Chunk<InternalNode>);
    }
  }

  private async _visitBTreeInternalNode(
    chunk: dag.Chunk<InternalNode>,
  ): Promise<void> {
    const {data} = chunk;
    await Promise.all(
      data[1].map(entry => this.visitBTreeNode(entry[1] as Hash)),
    );
  }

  private async _visitCommitIndexes(
    indexes: readonly IndexRecord[],
  ): Promise<void> {
    await Promise.all(
      indexes.map(async index => this.visitBTreeNode(index.valueHash)),
    );
  }
}
