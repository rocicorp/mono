import {internalizeBTreeNode, isInternalNode} from '../btree/node';
import * as btree from '../btree/mod';
import {
  IndexChangeMeta,
  Meta,
  MetaType,
  SnapshotMeta,
  getRefs as getRefsFromCommitData,
  assertCommitData,
  LocalMeta,
  CommitData,
  IndexRecord,
  LocalMetaDD31,
  isLocalMetaDD31,
  SnapshotMetaDD31,
  isSnapshotMetaDD31,
} from './commit';
import type {Hash} from '../hash';
import type * as dag from '../dag/mod';
import {HashRefType} from './hash-ref-type';
import {mustGetChunk} from '../dag/store.js';
import type {InternalValue} from '../internal-value.js';

type OldHash = Hash;
type NewHash = Hash;

export abstract class BaseTransformer {
  private readonly _transforming: Map<OldHash, Promise<NewHash>> = new Map();
  private readonly _writtenMappings = new Map<OldHash, NewHash>();

  // no constructor as the base class doesn't take a dag.Read or dag.Write

  /**
   * This is a mapping from the old hash to the new hash for all the chunks that
   * were written.
   */
  get mappings(): ReadonlyMap<OldHash, NewHash> {
    return this._writtenMappings;
  }

  private _withTransformingCache(
    oldHash: OldHash,
    f: () => Promise<Hash>,
  ): Promise<NewHash> {
    const newHash = this._transforming.get(oldHash);
    if (newHash !== undefined) {
      return newHash;
    }

    const p = f();
    this._transforming.set(oldHash, p);
    return p;
  }

  private _transformCommitWithCache(
    oldHash: OldHash,
    hashRefType = HashRefType.RequireStrong,
  ): Promise<NewHash> {
    return this._withTransformingCache(oldHash, () =>
      this.transformCommit(oldHash, hashRefType),
    );
  }

  async transformCommit(
    oldHash: OldHash,
    hashRefType = HashRefType.RequireStrong,
  ): Promise<NewHash> {
    if (this.shouldSkip(oldHash)) {
      return oldHash;
    }

    const chunk = await this.getChunk(oldHash);
    if (!chunk) {
      if (hashRefType === HashRefType.AllowWeak) {
        return oldHash;
      }
      throw new Error(`Chunk ${oldHash} not found`);
    }
    const {data} = chunk;
    assertCommitData(data);

    const newCommitData = await this._transformCommitData(data);
    return this._maybeWriteChunk(
      oldHash,
      newCommitData,
      data,
      getRefsFromCommitData,
    );
  }

  protected shouldSkip(_oldHash: OldHash): boolean {
    return false;
  }

  protected shouldForceWrite(_oldHash: OldHash): boolean {
    return false;
  }

  abstract getChunk(oldHash: OldHash): Promise<dag.Chunk | undefined>;

  async mustGetChunk(oldHash: OldHash): Promise<dag.Chunk> {
    return mustGetChunk(this, oldHash);
  }

  private async _maybeWriteChunk<D>(
    oldHash: OldHash,
    newData: D,
    oldData: D,
    getRefs: (data: D) => readonly NewHash[],
  ): Promise<NewHash> {
    if (newData !== oldData || this.shouldForceWrite(oldHash)) {
      const newHash = await this.writeChunk(oldHash, newData, getRefs);
      this._writtenMappings.set(oldHash, newHash);
      return newHash;
    }
    return oldHash;
  }

  protected abstract writeChunk<D>(
    oldHash: OldHash,
    newData: D,
    getRefs: (data: D) => readonly NewHash[],
  ): Promise<NewHash>;

  private async _transformCommitData<M extends Meta>(
    data: CommitData<M>,
  ): Promise<CommitData<Meta>> {
    const [meta, valueHash, indexes] = await Promise.all([
      this._transformCommitMeta(data.meta),
      this._transformCommitValue(data.valueHash),
      this._transformIndexRecords(data.indexes),
    ]);

    if (
      meta === data.meta &&
      valueHash === data.valueHash &&
      indexes === data.indexes
    ) {
      return data;
    }
    return {
      meta,
      valueHash,
      indexes,
    };
  }

  private _transformCommitMeta<M extends Meta>(meta: M): Promise<Meta> {
    switch (meta.type) {
      case MetaType.IndexChange:
        return this._transformIndexChangeMeta(meta);

      case MetaType.Local:
        return this._transformLocalMeta(meta);

      case MetaType.Snapshot:
        return this._transformSnapshot(meta);
    }
  }

  private _transformBasisHash(
    basisHash: OldHash,
    hashRefType: HashRefType,
  ): Promise<NewHash>;
  private _transformBasisHash(basisHash: null, hashRefType: HashRefType): null;
  private _transformBasisHash(
    basisHash: OldHash | null,
    hashRefType: HashRefType,
  ): Promise<NewHash> | null;
  private _transformBasisHash(
    basisHash: OldHash | null,
    hashRefType: HashRefType,
  ): Promise<NewHash> | null {
    if (basisHash !== null) {
      return this._transformCommitWithCache(basisHash, hashRefType);
    }
    return null;
  }

  private async _transformSnapshot(
    meta: SnapshotMeta | SnapshotMetaDD31,
  ): Promise<SnapshotMeta | SnapshotMetaDD31> {
    // basisHash is weak for Snapshot Commits
    const basisHash = await this._transformBasisHash(
      meta.basisHash,
      HashRefType.AllowWeak,
    );
    if (basisHash === meta.basisHash) {
      return meta;
    }
    if (isSnapshotMetaDD31(meta)) {
      return {
        basisHash,
        type: meta.type,
        lastMutationIDs: meta.lastMutationIDs,
        cookieJSON: meta.cookieJSON,
      };
    }
    return {
      basisHash,
      type: (meta as SnapshotMeta).type,
      lastMutationID: (meta as SnapshotMeta).lastMutationID,
      cookieJSON: (meta as SnapshotMeta).cookieJSON,
    };
  }

  private _transformLocalMeta(meta: LocalMetaDD31): Promise<LocalMetaDD31>;
  private _transformLocalMeta(meta: LocalMeta): Promise<LocalMeta>;
  private async _transformLocalMeta(
    meta: LocalMeta | LocalMetaDD31,
  ): Promise<LocalMeta | LocalMetaDD31> {
    const basisHashP = this._transformBasisHash(
      meta.basisHash,
      HashRefType.RequireStrong,
    );
    // originalHash is weak for Local Commits
    const originalHashP =
      meta.originalHash &&
      this._transformCommitWithCache(meta.originalHash, HashRefType.AllowWeak);

    const basisHash = await basisHashP;
    const originalHash = await originalHashP;

    if (basisHash === meta.basisHash && originalHash === meta.originalHash) {
      return meta;
    }

    if (DD31 && isLocalMetaDD31(meta)) {
      return {
        basisHash,
        type: meta.type,
        mutationID: meta.mutationID,
        mutatorName: meta.mutatorName,
        mutatorArgsJSON: meta.mutatorArgsJSON,
        originalHash,
        timestamp: meta.timestamp,
        clientID: meta.clientID,
      };
    }

    return {
      basisHash,
      type: meta.type,
      mutationID: meta.mutationID,
      mutatorName: meta.mutatorName,
      mutatorArgsJSON: meta.mutatorArgsJSON,
      originalHash,
      timestamp: meta.timestamp,
    };
  }

  private async _transformIndexChangeMeta(
    meta: IndexChangeMeta,
  ): Promise<IndexChangeMeta> {
    const basisHash = await this._transformBasisHash(
      meta.basisHash,
      HashRefType.RequireStrong,
    );
    if (basisHash === meta.basisHash) {
      return meta;
    }
    return {
      basisHash,
      type: meta.type,
      lastMutationID: meta.lastMutationID,
    };
  }

  private _transformCommitValue(valueHash: OldHash): Promise<NewHash> {
    return this._transformBTreeNodeWithCache(valueHash);
  }

  private _transformBTreeNodeWithCache(h: OldHash): Promise<NewHash> {
    return this._withTransformingCache(h, () => this.transformBTreeNode(h));
  }

  async transformBTreeNode(oldHash: OldHash): Promise<NewHash> {
    if (this.shouldSkip(oldHash)) {
      return oldHash;
    }

    const {data} = await this.mustGetChunk(oldHash);
    internalizeBTreeNode(data);

    const newData = await this.transformBTreeNodeData(data);
    return this._maybeWriteChunk(oldHash, newData, data, btree.getRefs);
  }

  transformBTreeNodeData(data: btree.DataNode): Promise<btree.DataNode>;
  transformBTreeNodeData(data: btree.InternalNode): Promise<btree.InternalNode>;
  transformBTreeNodeData(data: btree.Node): Promise<btree.Node>;
  async transformBTreeNodeData(data: btree.Node): Promise<btree.Node> {
    const level = data[0];
    const entries = data[1];
    let newEntries: btree.Node[1];
    if (isInternalNode(data)) {
      newEntries = await this._transformBTreeInternalEntries(
        entries as btree.InternalNode[1],
      );
    } else {
      newEntries = await this._transformBTreeDataEntries(
        entries as btree.DataNode[1],
      );
    }
    if (newEntries === entries) {
      return data;
    }

    // Changed. Need to create a new chunk.
    return [level, newEntries] as btree.Node;
  }

  async transformBTreeDataEntry(
    entry: btree.Entry<InternalValue>,
  ): Promise<btree.Entry<InternalValue>> {
    return entry;
  }

  private async _transformBTreeDataEntries(
    entries: readonly btree.Entry<InternalValue>[],
  ): Promise<readonly btree.Entry<InternalValue>[]> {
    return this._transformArray(entries, e => this.transformBTreeDataEntry(e));
  }

  async transformBTreeInternalEntry(
    entry: btree.Entry<OldHash>,
  ): Promise<btree.Entry<NewHash>> {
    const hash = await this._transformBTreeNodeWithCache(entry[1]);
    if (hash === entry[1]) {
      return entry;
    }
    return [entry[0], hash];
  }

  private async _transformBTreeInternalEntries(
    entries: readonly btree.Entry<OldHash>[],
  ): Promise<readonly btree.Entry<NewHash>[]> {
    return this._transformArray(entries, e =>
      this.transformBTreeInternalEntry(e),
    );
  }

  private async _transformArray<T>(
    values: readonly T[],
    itemTransform: (item: T) => Promise<T>,
  ): Promise<readonly T[]> {
    const newValues = await Promise.all(values.map(itemTransform));
    for (let i = 0; i < newValues.length; i++) {
      if (values[i] !== newValues[i]) {
        return newValues;
      }
    }
    return values;
  }

  private _transformIndexRecords(
    indexes: readonly IndexRecord[],
  ): Promise<readonly IndexRecord[]> {
    return this._transformArray(indexes, i => this.transformIndexRecord(i));
  }

  async transformIndexRecord(index: IndexRecord): Promise<IndexRecord> {
    const valueHash = await this._transformBTreeNodeWithCache(index.valueHash);
    if (valueHash === index.valueHash) {
      return index;
    }
    return {
      definition: index.definition,
      valueHash,
    };
  }
}

export class Transformer extends BaseTransformer {
  protected readonly dagWrite: dag.Write;

  constructor(dagWrite: dag.Write) {
    super();
    this.dagWrite = dagWrite;
  }

  override getChunk(oldHash: OldHash): Promise<dag.Chunk | undefined> {
    return this.dagWrite.getChunk(oldHash);
  }

  protected override async writeChunk<D>(
    _oldHash: OldHash,
    newData: D,
    getRefs: (data: D) => readonly NewHash[],
  ): Promise<NewHash> {
    const {dagWrite} = this;
    const newChunk = dagWrite.createChunk(newData, getRefs(newData));
    await dagWrite.putChunk(newChunk);
    return newChunk.hash;
  }
}
