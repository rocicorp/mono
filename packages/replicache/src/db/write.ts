import type {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/asserts.js';
import * as btree from '../btree/mod.js';
import {BTreeRead, BTreeWrite} from '../btree/mod.js';
import type {InternalDiff} from '../btree/node.js';
import {allEntriesAsDiff} from '../btree/read.js';
import type {FrozenCookie} from '../cookies.js';
import type * as dag from '../dag/mod.js';
import {Hash, emptyHash} from '../hash.js';
import type {FrozenJSONValue} from '../json.js';
import {lazy} from '../lazy.js';
import type {DiffComputationConfig} from '../sync/diff.js';
import type {ClientID} from '../sync/ids.js';
import * as sync from '../sync/mod.js';
import {
  Commit,
  Meta as CommitMeta,
  IndexRecord,
  Meta,
  MetaType,
  baseSnapshotHashFromHash,
  newIndexChange as commitNewIndexChange,
  newLocalDD31 as commitNewLocalDD31,
  newLocalSDD as commitNewLocalSDD,
  newSnapshotDD31 as commitNewSnapshotDD31,
  newSnapshotSDD as commitNewSnapshotSDD,
  getMutationID,
} from './commit.js';
import {IndexOperation, IndexRead, IndexWrite, indexValue} from './index.js';
import {
  Read,
  Whence,
  readCommitForBTreeWrite,
  readIndexesForRead,
} from './read.js';

export class Write extends Read {
  private readonly _dagWrite: dag.Write;
  private readonly _basis: Commit<CommitMeta> | undefined;
  private readonly _meta: CommitMeta;

  declare map: BTreeWrite;

  declare readonly indexes: Map<string, IndexWrite>;
  private readonly _clientID: ClientID;
  private readonly _dd31: boolean;

  constructor(
    dagWrite: dag.Write,
    map: BTreeWrite,
    basis: Commit<CommitMeta> | undefined,
    meta: CommitMeta,
    indexes: Map<string, IndexWrite>,
    clientID: ClientID,
    dd31: boolean,
  ) {
    // TypeScript has trouble
    super(dagWrite, map, indexes);
    this._dagWrite = dagWrite;
    this._basis = basis;
    this._meta = meta;
    this._clientID = clientID;
    this._dd31 = dd31;

    // TODO(arv): if (DEBUG) { ...
    if (basis === undefined) {
      assert(meta.basisHash === emptyHash);
    } else {
      assert(meta.basisHash === basis.chunk.hash);
    }
  }

  /**
   * The value needs to be frozen since it is kept in memory and used later for
   * comparison as well as returned in `get`.
   */
  async put(
    lc: LogContext,
    key: string,
    value: FrozenJSONValue,
  ): Promise<void> {
    if (this._meta.type === MetaType.IndexChangeSDD) {
      throw new Error('Not allowed');
    }
    const oldVal = lazy(() => this.map.get(key));
    await updateIndexes(lc, this.indexes, key, oldVal, value);

    await this.map.put(key, value);
  }

  getMutationID(): Promise<number> {
    return getMutationID(this._clientID, this._dagWrite, this._meta);
  }

  async del(lc: LogContext, key: string): Promise<boolean> {
    if (this._meta.type === MetaType.IndexChangeSDD) {
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
    if (this._meta.type === MetaType.IndexChangeSDD) {
      throw new Error('Not allowed');
    }

    await this.map.clear();
    const ps = [];
    for (const idx of this.indexes.values()) {
      ps.push(idx.clear());
    }
    await Promise.all(ps);
  }

  async putCommit(): Promise<Commit<CommitMeta>> {
    const valueHash = await this.map.flush();
    const indexRecords: IndexRecord[] = [];

    for (const index of this.indexes.values()) {
      const valueHash = await index.flush();
      const indexRecord: IndexRecord = {
        definition: index.meta.definition,
        valueHash,
      };
      indexRecords.push(indexRecord);
    }

    let commit: Commit<Meta>;
    const meta = this._meta;
    switch (meta.type) {
      case MetaType.LocalSDD: {
        const {
          basisHash,
          mutationID,
          mutatorName,
          mutatorArgsJSON,
          originalHash,
          timestamp,
        } = meta;
        commit = commitNewLocalSDD(
          this._dagWrite.createChunk,
          basisHash,
          mutationID,
          mutatorName,
          mutatorArgsJSON,
          originalHash,
          valueHash,
          indexRecords,
          timestamp,
        );
        break;
      }

      case MetaType.LocalDD31: {
        assert(this._dd31);
        const {
          basisHash,
          mutationID,
          mutatorName,
          mutatorArgsJSON,
          originalHash,
          timestamp,
        } = meta;
        commit = commitNewLocalDD31(
          this._dagWrite.createChunk,
          basisHash,
          await baseSnapshotHashFromHash(basisHash, this._dagWrite),
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

      case MetaType.SnapshotSDD: {
        assert(!this._dd31);
        const {basisHash, lastMutationID, cookieJSON} = meta;
        commit = commitNewSnapshotSDD(
          this._dagWrite.createChunk,
          basisHash,
          lastMutationID,
          cookieJSON,
          valueHash,
          indexRecords,
        );
        break;
      }

      case MetaType.SnapshotDD31: {
        assert(this._dd31);
        const {basisHash, lastMutationIDs, cookieJSON} = meta;
        commit = commitNewSnapshotDD31(
          this._dagWrite.createChunk,
          basisHash,
          lastMutationIDs,
          cookieJSON,
          valueHash,
          indexRecords,
        );
        break;
      }

      case MetaType.IndexChangeSDD: {
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
    await this._dagWrite.putChunk(commit.chunk);
    return commit;
  }

  // Return value is the hash of the new commit.
  async commit(headName: string): Promise<Hash> {
    const commit = await this.putCommit();
    const commitHash = commit.chunk.hash;
    await this._dagWrite.setHead(headName, commitHash);
    await this._dagWrite.commit();
    return commitHash;
  }

  async commitWithDiffs(
    headName: string,
    diffConfig: DiffComputationConfig,
  ): Promise<[Hash, sync.DiffsMap]> {
    const commit = this.putCommit();
    const diffMap = await this._generateDiffs(diffConfig);
    const commitHash = (await commit).chunk.hash;
    await this._dagWrite.setHead(headName, commitHash);
    await this._dagWrite.commit();
    return [commitHash, diffMap];
  }

  private async _generateDiffs(
    diffConfig: DiffComputationConfig,
  ): Promise<sync.DiffsMap> {
    const diffsMap = new sync.DiffsMap();
    if (!diffConfig.shouldComputeDiffs()) {
      return diffsMap;
    }

    let valueDiff: InternalDiff = [];
    if (this._basis) {
      const basisMap = new BTreeRead(this._dagWrite, this._basis.valueHash);
      valueDiff = await btree.diff(basisMap, this.map);
    }
    diffsMap.set('', valueDiff);
    let basisIndexes: Map<string, IndexRead>;
    if (this._basis) {
      basisIndexes = readIndexesForRead(this._basis, this._dagWrite);
    } else {
      basisIndexes = new Map();
    }

    for (const [name, index] of this.indexes) {
      if (!diffConfig.shouldComputeDiffsForIndex(name)) {
        continue;
      }
      const basisIndex = basisIndexes.get(name);
      assert(index !== basisIndex);

      const indexDiffResult = await (basisIndex
        ? btree.diff(basisIndex.map, index.map)
        : // No basis. All keys are new.
          allEntriesAsDiff(index.map, 'add'));
      diffsMap.set(name, indexDiffResult);
    }

    // Handle indexes in basisIndex but not in this.indexes. All keys are
    // deleted.
    for (const [name, basisIndex] of basisIndexes) {
      if (
        !this.indexes.has(name) &&
        diffConfig.shouldComputeDiffsForIndex(name)
      ) {
        const indexDiffResult = await allEntriesAsDiff(basisIndex.map, 'del');
        diffsMap.set(name, indexDiffResult);
      }
    }
    return diffsMap;
  }

  close(): void {
    this._dagWrite.release();
  }
}

export async function newWriteLocal(
  whence: Whence,
  mutatorName: string,
  mutatorArgsJSON: FrozenJSONValue,
  originalHash: Hash | null,
  dagWrite: dag.Write,
  timestamp: number,
  clientID: ClientID,
  dd31: boolean,
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
    dd31
      ? {
          type: MetaType.LocalDD31,
          basisHash,
          baseSnapshotHash: await baseSnapshotHashFromHash(basisHash, dagWrite),
          mutatorName,
          mutatorArgsJSON,
          mutationID,
          originalHash,
          timestamp,
          clientID,
        }
      : {
          type: MetaType.LocalSDD,
          basisHash,
          mutatorName,
          mutatorArgsJSON,
          mutationID,
          originalHash,
          timestamp,
        },
    indexes,
    clientID,
    dd31,
  );
}

export async function newWriteSnapshotSDD(
  whence: Whence,
  lastMutationID: number,
  cookieJSON: FrozenJSONValue,
  dagWrite: dag.Write,
  indexes: Map<string, IndexWrite>,
  clientID: ClientID,
): Promise<Write> {
  const [, basis, bTreeWrite] = await readCommitForBTreeWrite(whence, dagWrite);
  const basisHash = basis.chunk.hash;
  return new Write(
    dagWrite,
    bTreeWrite,
    basis,
    {basisHash, type: MetaType.SnapshotSDD, lastMutationID, cookieJSON},
    indexes,
    clientID,
    false,
  );
}

export async function newWriteSnapshotDD31(
  whence: Whence,
  lastMutationIDs: Record<ClientID, number>,
  cookieJSON: FrozenCookie,
  dagWrite: dag.Write,
  clientID: ClientID,
): Promise<Write> {
  const [basisHash, basis, bTreeWrite] = await readCommitForBTreeWrite(
    whence,
    dagWrite,
  );
  return new Write(
    dagWrite,
    bTreeWrite,
    basis,
    {basisHash, type: MetaType.SnapshotDD31, lastMutationIDs, cookieJSON},
    readIndexesForWrite(basis, dagWrite),
    clientID,
    true,
  );
}

export async function updateIndexes(
  lc: LogContext,
  indexes: Map<string, IndexWrite>,
  key: string,
  oldValGetter: () => Promise<FrozenJSONValue | undefined>,
  newVal: FrozenJSONValue | undefined,
): Promise<void> {
  const ps: Promise<void>[] = [];
  for (const idx of indexes.values()) {
    const {keyPrefix} = idx.meta.definition;
    if (!keyPrefix || key.startsWith(keyPrefix)) {
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
            idx.meta.definition.allowEmpty ?? false,
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
            idx.meta.definition.allowEmpty ?? false,
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

export async function createIndexBTree(
  lc: LogContext,
  dagWrite: dag.Write,
  valueMap: BTreeRead,
  prefix: string,
  jsonPointer: string,
  allowEmpty: boolean,
): Promise<BTreeWrite> {
  const indexMap = new BTreeWrite(dagWrite);
  for await (const entry of valueMap.scan(prefix)) {
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
  return indexMap;
}
