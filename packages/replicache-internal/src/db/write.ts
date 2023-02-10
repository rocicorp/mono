import type {LogContext} from '@rocicorp/logger';
import type * as dag from '../dag/mod';
import * as btree from '../btree/mod';
import {
  Commit,
  Meta as CommitMeta,
  IndexDefinition,
  IndexRecord,
  newIndexChange as commitNewIndexChange,
  newLocal as commitNewLocal,
  newSnapshot as commitNewSnapshot,
  newSnapshotDD31 as commitNewSnapshotDD31,
  MetaType,
  assertSnapshotMetaDD31,
  assertSnapshotMeta,
} from './commit';
import {
  Read,
  readCommitForBTreeWrite,
  readIndexesForRead,
  Whence,
} from './read';
import {IndexWrite, IndexOperation, indexValue, IndexRead} from './index';
import {BTreeRead, BTreeWrite} from '../btree/mod';
import {lazy} from '../lazy';
import {emptyHash, Hash} from '../hash';
import type {InternalDiff} from '../btree/node.js';
import {allEntriesAsDiff} from '../btree/read.js';
import type {ClientID, DiffsMap} from '../sync/mod.js';
import type {InternalValue} from '../internal-value.js';
import {assert} from '../asserts.js';

export class Write extends Read {
  private readonly _dagWrite: dag.Write;
  private readonly _basis: Commit<CommitMeta> | undefined;
  private readonly _meta: CommitMeta;

  shouldDeepClone = true;

  declare map: BTreeWrite;

  declare readonly indexes: Map<string, IndexWrite>;
  private readonly _clientID: string;

  constructor(
    dagWrite: dag.Write,
    map: BTreeWrite,
    basis: Commit<CommitMeta> | undefined,
    meta: CommitMeta,
    indexes: Map<string, IndexWrite>,
    clientID: ClientID,
  ) {
    // TypeScript has trouble
    super(dagWrite, map, indexes);
    this._dagWrite = dagWrite;
    this._basis = basis;
    this._meta = meta;
    if (DD31 && meta.type === MetaType.Snapshot) {
      assertSnapshotMetaDD31(meta);
    }
    this._clientID = clientID;

    // TODO(arv): if (DEBUG) { ...
    if (basis === undefined) {
      assert(meta.basisHash === emptyHash);
    } else {
      assert(meta.basisHash === basis.chunk.hash);
    }
  }

  isRebase(): boolean {
    return (
      this._meta.type === MetaType.Local && this._meta.originalHash !== null
    );
  }

  async put(lc: LogContext, key: string, val: InternalValue): Promise<void> {
    if (this._meta.type === MetaType.IndexChange) {
      throw new Error('Not allowed');
    }
    const oldVal = lazy(() => this.map.get(key));
    await updateIndexes(lc, this.indexes, key, oldVal, val);

    await this.map.put(key, val);
  }

  async del(lc: LogContext, key: string): Promise<boolean> {
    if (this._meta.type === MetaType.IndexChange) {
      throw new Error('Not allowed');
    }

    // TODO(arv): This does the binary search twice. We can do better.
    const oldVal = lazy(() => this.map.get(key));
    if (oldVal !== undefined) {
      await updateIndexes(lc, this.indexes, key, oldVal, undefined);
    }
    return this.map.del(key);
  }

  async clear(): Promise<void> {
    if (this._meta.type === MetaType.IndexChange) {
      throw new Error('Not allowed');
    }

    await this.map.clear();
    const ps = [];
    for (const idx of this.indexes.values()) {
      ps.push(idx.clear());
    }
    await Promise.all(ps);
  }

  async createIndex(
    lc: LogContext,
    name: string,
    keyPrefix: string,
    jsonPointer: string,
    allowEmpty: boolean,
  ): Promise<void> {
    if (this._meta.type === MetaType.Local) {
      throw new Error('Not allowed');
    }

    const definition: IndexDefinition = {
      name,
      keyPrefix,
      jsonPointer,
      allowEmpty,
    };

    // Check to see if the index already exists.
    const index = this.indexes.get(name);
    if (index) {
      const oldDefinition = index.meta.definition;
      const oldAllowEmpty = oldDefinition.allowEmpty ?? false;
      if (
        oldDefinition.name === name &&
        oldDefinition.keyPrefix === keyPrefix &&
        oldDefinition.jsonPointer === jsonPointer &&
        oldAllowEmpty === allowEmpty
      ) {
        return;
      } else {
        throw new Error('Index exists with different definition');
      }
    }

    const indexMap = new BTreeWrite(this._dagWrite);
    for await (const entry of this.map.scan(keyPrefix)) {
      await indexValue(
        lc,
        indexMap,
        IndexOperation.Add,
        entry[0],
        entry[1],
        jsonPointer,
        allowEmpty,
      );
    }

    this.indexes.set(
      name,
      new IndexWrite(
        {
          definition,
          valueHash: emptyHash,
        },
        indexMap,
      ),
    );
  }

  async dropIndex(name: string): Promise<void> {
    if (this._meta.type === MetaType.Local) {
      throw new Error('Not allowed');
    }

    if (!this.indexes.delete(name)) {
      throw new Error(`No such index: ${name}`);
    }
  }

  // Return value is the hash of the new commit.
  async commit(headName: string): Promise<Hash> {
    return (await this.commitWithDiffs(headName, false, true))[0];
  }

  async commitWithDiffs(
    headName: string,
    generateDiffs: boolean,
    commitDagWrite: boolean,
  ): Promise<[Hash, DiffsMap]> {
    const valueHash = await this.map.flush();
    let valueDiff: InternalDiff = [];
    if (generateDiffs && this._basis) {
      const basisMap = new BTreeRead(this._dagWrite, this._basis.valueHash);
      valueDiff = await btree.diff(basisMap, this.map);
    }
    const indexRecords: IndexRecord[] = [];
    const diffMap: Map<string, InternalDiff> = new Map();
    if (valueDiff.length > 0) {
      diffMap.set('', valueDiff);
    }

    let basisIndexes: Map<string, IndexRead>;
    if (generateDiffs && this._basis) {
      basisIndexes = readIndexesForRead(this._basis, this._dagWrite);
    } else {
      basisIndexes = new Map();
    }

    for (const [name, index] of this.indexes) {
      const valueHash = await index.flush();
      if (generateDiffs) {
        const basisIndex = basisIndexes.get(name);
        assert(index !== basisIndex);

        const indexDiffResult = await (basisIndex
          ? btree.diff(basisIndex.map, index.map)
          : // No basis. All keys are new.
            allEntriesAsDiff(index.map, 'add'));

        if (indexDiffResult.length > 0) {
          diffMap.set(name, indexDiffResult);
        }
      }
      const indexRecord: IndexRecord = {
        definition: index.meta.definition,
        valueHash,
      };
      indexRecords.push(indexRecord);
    }

    if (generateDiffs) {
      // Handle indexes in basisIndex but not in this.indexes. All keys are
      // deleted.
      for (const [name, basisIndex] of basisIndexes) {
        if (!this.indexes.has(name)) {
          const indexDiffResult = await allEntriesAsDiff(basisIndex.map, 'del');
          if (indexDiffResult.length > 0) {
            diffMap.set(name, indexDiffResult);
          }
        }
      }
    }

    let commit;
    const meta = this._meta;
    switch (meta.type) {
      case MetaType.Local: {
        const {
          basisHash,
          mutationID,
          mutatorName,
          mutatorArgsJSON,
          originalHash,
          timestamp,
        } = meta;
        commit = commitNewLocal(
          this._dagWrite.createChunk,
          basisHash,
          mutationID,
          mutatorName,
          mutatorArgsJSON,
          originalHash,
          valueHash,
          indexRecords,
          timestamp,
          this._clientID,
        );
        break;
      }
      case MetaType.Snapshot: {
        if (DD31) {
          assertSnapshotMetaDD31(meta);
          const {basisHash, lastMutationIDs, cookieJSON} = meta;
          commit = commitNewSnapshotDD31(
            this._dagWrite.createChunk,
            basisHash,
            lastMutationIDs,
            cookieJSON,
            valueHash,
            indexRecords,
          );
        } else {
          assertSnapshotMeta(meta);
          const {basisHash, lastMutationID, cookieJSON} = meta;
          commit = commitNewSnapshot(
            this._dagWrite.createChunk,
            basisHash,
            lastMutationID,
            cookieJSON,
            valueHash,
            indexRecords,
          );
        }
        break;
      }
      case MetaType.IndexChange: {
        const {basisHash, lastMutationID} = meta;
        if (this._basis !== undefined) {
          if (
            (await this._basis.getMutationID(
              this._clientID,
              this._dagWrite,
            )) !== lastMutationID
          ) {
            throw new Error('Index change must not change mutationID');
          }
          if (this._basis.valueHash !== valueHash) {
            throw new Error('Index change must not change valueHash');
          }
        }
        commit = commitNewIndexChange(
          this._dagWrite.createChunk,
          basisHash,
          lastMutationID,
          valueHash,
          indexRecords,
        );
        break;
      }
    }

    await Promise.all([
      this._dagWrite.putChunk(commit.chunk),
      this._dagWrite.setHead(headName, commit.chunk.hash),
    ]);

    if (commitDagWrite) {
      await this._dagWrite.commit();
    }

    return [commit.chunk.hash, diffMap];
  }

  close(): void {
    this._dagWrite.close();
  }
}

export async function newWriteLocal(
  whence: Whence,
  mutatorName: string,
  mutatorArgsJSON: InternalValue,
  originalHash: Hash | null,
  dagWrite: dag.Write,
  timestamp: number,
  clientID: ClientID,
): Promise<Write> {
  const [basisHash, basis, bTreeWrite] = await readCommitForBTreeWrite(
    whence,
    dagWrite,
  );

  const mutationID = await basis.getNextMutationID(clientID, dagWrite);
  const indexes = readIndexesForWrite(basis, dagWrite);
  return new Write(
    dagWrite,
    bTreeWrite,
    basis,
    DD31
      ? {
          type: MetaType.Local,
          basisHash,
          mutatorName,
          mutatorArgsJSON,
          mutationID,
          originalHash,
          timestamp,
          clientID,
        }
      : {
          type: MetaType.Local,
          basisHash,
          mutatorName,
          mutatorArgsJSON,
          mutationID,
          originalHash,
          timestamp,
        },
    indexes,
    clientID,
  );
}

export async function newWriteSnapshot(
  whence: Whence,
  lastMutationID: number,
  cookieJSON: InternalValue,
  dagWrite: dag.Write,
  indexes: Map<string, IndexWrite>,
  clientID: ClientID,
): Promise<Write> {
  assert(!DD31);
  const [, basis, bTreeWrite] = await readCommitForBTreeWrite(whence, dagWrite);
  const basisHash = basis.chunk.hash;
  return new Write(
    dagWrite,
    bTreeWrite,
    basis,
    {basisHash, type: MetaType.Snapshot, lastMutationID, cookieJSON},
    indexes,
    clientID,
  );
}

export async function newWriteSnapshotDD31(
  whence: Whence,
  lastMutationIDs: Record<ClientID, number>,
  cookieJSON: InternalValue,
  dagWrite: dag.Write,
  indexes: Map<string, IndexWrite>,
  clientID: ClientID,
): Promise<Write> {
  assert(DD31);
  const [basisHash, basis, bTreeWrite] = await readCommitForBTreeWrite(
    whence,
    dagWrite,
  );
  return new Write(
    dagWrite,
    bTreeWrite,
    basis,
    {basisHash, type: MetaType.Snapshot, lastMutationIDs, cookieJSON},
    indexes,
    clientID,
  );
}

export async function newWriteIndexChange(
  whence: Whence,
  dagWrite: dag.Write,
  clientID: ClientID,
): Promise<Write> {
  const [basisHash, basis, bTreeWrite] = await readCommitForBTreeWrite(
    whence,
    dagWrite,
  );
  const lastMutationID = await basis.getMutationID(clientID, dagWrite);
  const indexes = readIndexesForWrite(basis, dagWrite);
  return new Write(
    dagWrite,
    bTreeWrite,
    basis,
    {basisHash, type: MetaType.IndexChange, lastMutationID},
    indexes,
    clientID,
  );
}

export async function updateIndexes(
  lc: LogContext,
  indexes: Map<string, IndexWrite>,
  key: string,
  oldValGetter: () => Promise<InternalValue | undefined>,
  newVal: InternalValue | undefined,
): Promise<void> {
  const ps: Promise<void>[] = [];
  for (const idx of indexes.values()) {
    if (key.startsWith(idx.meta.definition.keyPrefix)) {
      const oldVal = await oldValGetter();
      if (oldVal !== undefined) {
        ps.push(
          indexValue(
            lc,
            idx.map,
            IndexOperation.Remove,
            key,
            oldVal,
            idx.meta.definition.jsonPointer,
            idx.meta.definition.allowEmpty,
          ),
        );
      }
      if (newVal !== undefined) {
        ps.push(
          indexValue(
            lc,
            idx.map,
            IndexOperation.Add,
            key,
            newVal,
            idx.meta.definition.jsonPointer,
            idx.meta.definition.allowEmpty,
          ),
        );
      }
    }
  }
  await Promise.all(ps);
}

export function readIndexesForWrite(
  commit: Commit<CommitMeta>,
  dagWrite: dag.Write,
): Map<string, IndexWrite> {
  const m = new Map();
  for (const index of commit.indexes) {
    m.set(
      index.definition.name,
      new IndexWrite(index, new BTreeWrite(dagWrite, index.valueHash)),
    );
  }
  return m;
}
