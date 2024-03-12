import type {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import {diff} from '../btree/diff.js';
import type {InternalDiff} from '../btree/node.js';
import {BTreeRead, allEntriesAsDiff} from '../btree/read.js';
import {BTreeWrite} from '../btree/write.js';
import type {FrozenCookie} from '../cookies.js';
import type {Write as DagWrite} from '../dag/store.js';
import {FormatVersion} from '../format-version.js';
import type {FrozenJSONValue} from '../frozen-json.js';
import {Hash, emptyHash} from '../hash.js';
import {lazy} from '../lazy.js';
import type {DiffComputationConfig} from '../sync/diff.js';
import {DiffsMap} from '../sync/diff.js';
import type {ClientID} from '../sync/ids.js';
import {
  Commit,
  Meta as CommitMeta,
  IndexRecord,
  Meta,
  MetaType,
  baseSnapshotHashFromHash,
  commitFromHash,
  newIndexChange as commitNewIndexChange,
  newLocalDD31 as commitNewLocalDD31,
  newLocalSDD as commitNewLocalSDD,
  newSnapshotDD31 as commitNewSnapshotDD31,
  newSnapshotSDD as commitNewSnapshotSDD,
  getMutationID,
} from './commit.js';
import {IndexOperation, IndexRead, IndexWrite, indexValue} from './index.js';
import {Read, readIndexesForRead} from './read.js';

export class Write extends Read {
  readonly #dagWrite: DagWrite;
  readonly #basis: Commit<CommitMeta> | undefined;
  readonly #meta: CommitMeta;

  declare map: BTreeWrite;

  declare readonly indexes: Map<string, IndexWrite>;
  readonly #clientID: ClientID;
  readonly #formatVersion: FormatVersion;

  constructor(
    dagWrite: DagWrite,
    map: BTreeWrite,
    basis: Commit<CommitMeta> | undefined,
    meta: CommitMeta,
    indexes: Map<string, IndexWrite>,
    clientID: ClientID,
    formatVersion: FormatVersion,
  ) {
    // TypeScript has trouble
    super(dagWrite, map, indexes);
    this.#dagWrite = dagWrite;
    this.#basis = basis;
    this.#meta = meta;
    this.#clientID = clientID;
    this.#formatVersion = formatVersion;

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
    if (this.#meta.type === MetaType.IndexChangeSDD) {
      throw new Error('Not allowed');
    }
    const oldVal = lazy(() => this.map.get(key));
    await updateIndexes(lc, this.indexes, key, oldVal, value);

    await this.map.put(key, value);
  }

  getMutationID(): Promise<number> {
    return getMutationID(this.#clientID, this.#dagWrite, this.#meta);
  }

  async del(lc: LogContext, key: string): Promise<boolean> {
    if (this.#meta.type === MetaType.IndexChangeSDD) {
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
    if (this.#meta.type === MetaType.IndexChangeSDD) {
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
    const meta = this.#meta;
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
          this.#dagWrite.createChunk,
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
        assert(this.#formatVersion >= FormatVersion.DD31);
        const {
          basisHash,
          mutationID,
          mutatorName,
          mutatorArgsJSON,
          originalHash,
          timestamp,
        } = meta;
        commit = commitNewLocalDD31(
          this.#dagWrite.createChunk,
          basisHash,
          await baseSnapshotHashFromHash(basisHash, this.#dagWrite),
          mutationID,
          mutatorName,
          mutatorArgsJSON,
          originalHash,
          valueHash,
          indexRecords,
          timestamp,
          this.#clientID,
        );
        break;
      }

      case MetaType.SnapshotSDD: {
        assert(this.#formatVersion <= FormatVersion.SDD);
        const {basisHash, lastMutationID, cookieJSON} = meta;
        commit = commitNewSnapshotSDD(
          this.#dagWrite.createChunk,
          basisHash,
          lastMutationID,
          cookieJSON,
          valueHash,
          indexRecords,
        );
        break;
      }

      case MetaType.SnapshotDD31: {
        assert(this.#formatVersion > FormatVersion.DD31);
        const {basisHash, lastMutationIDs, cookieJSON} = meta;
        commit = commitNewSnapshotDD31(
          this.#dagWrite.createChunk,
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
        if (this.#basis !== undefined) {
          if (
            (await this.#basis.getMutationID(
              this.#clientID,
              this.#dagWrite,
            )) !== lastMutationID
          ) {
            throw new Error('Index change must not change mutationID');
          }
          if (this.#basis.valueHash !== valueHash) {
            throw new Error('Index change must not change valueHash');
          }
        }
        commit = commitNewIndexChange(
          this.#dagWrite.createChunk,
          basisHash,
          lastMutationID,
          valueHash,
          indexRecords,
        );
        break;
      }
    }
    await this.#dagWrite.putChunk(commit.chunk);
    return commit;
  }

  // Return value is the hash of the new commit.
  async commit(headName: string): Promise<Hash> {
    const commit = await this.putCommit();
    const commitHash = commit.chunk.hash;
    await this.#dagWrite.setHead(headName, commitHash);
    await this.#dagWrite.commit();
    return commitHash;
  }

  async commitWithDiffs(
    headName: string,
    diffConfig: DiffComputationConfig,
  ): Promise<DiffsMap> {
    const commit = this.putCommit();
    const diffMap = await this.#generateDiffs(diffConfig);
    const commitHash = (await commit).chunk.hash;
    await this.#dagWrite.setHead(headName, commitHash);
    await this.#dagWrite.commit();
    return diffMap;
  }

  async #generateDiffs(diffConfig: DiffComputationConfig): Promise<DiffsMap> {
    const diffsMap = new DiffsMap();
    if (!diffConfig.shouldComputeDiffs()) {
      return diffsMap;
    }

    let valueDiff: InternalDiff = [];
    if (this.#basis) {
      const basisMap = new BTreeRead(
        this.#dagWrite,
        this.#formatVersion,
        this.#basis.valueHash,
      );
      valueDiff = await diff(basisMap, this.map);
    }
    diffsMap.set('', valueDiff);
    let basisIndexes: Map<string, IndexRead>;
    if (this.#basis) {
      basisIndexes = readIndexesForRead(
        this.#basis,
        this.#dagWrite,
        this.#formatVersion,
      );
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
        ? diff(basisIndex.map, index.map)
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
    this.#dagWrite.release();
  }
}

export async function newWriteLocal(
  basisHash: Hash,
  mutatorName: string,
  mutatorArgsJSON: FrozenJSONValue,
  originalHash: Hash | null,
  dagWrite: DagWrite,
  timestamp: number,
  clientID: ClientID,
  formatVersion: FormatVersion,
): Promise<Write> {
  const basis = await commitFromHash(basisHash, dagWrite);
  const bTreeWrite = new BTreeWrite(dagWrite, formatVersion, basis.valueHash);
  const mutationID = await basis.getNextMutationID(clientID, dagWrite);
  const indexes = readIndexesForWrite(basis, dagWrite, formatVersion);
  return new Write(
    dagWrite,
    bTreeWrite,
    basis,
    formatVersion >= FormatVersion.DD31
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
    formatVersion,
  );
}

export async function newWriteSnapshotSDD(
  basisHash: Hash,
  lastMutationID: number,
  cookieJSON: FrozenJSONValue,
  dagWrite: DagWrite,
  indexes: Map<string, IndexWrite>,
  clientID: ClientID,
  formatVersion: FormatVersion,
): Promise<Write> {
  assert(formatVersion <= FormatVersion.SDD);
  const basis = await commitFromHash(basisHash, dagWrite);
  const bTreeWrite = new BTreeWrite(dagWrite, formatVersion, basis.valueHash);
  return new Write(
    dagWrite,
    bTreeWrite,
    basis,
    {basisHash, type: MetaType.SnapshotSDD, lastMutationID, cookieJSON},
    indexes,
    clientID,
    formatVersion,
  );
}

export async function newWriteSnapshotDD31(
  basisHash: Hash,
  lastMutationIDs: Record<ClientID, number>,
  cookieJSON: FrozenCookie,
  dagWrite: DagWrite,
  clientID: ClientID,
  formatVersion: FormatVersion,
): Promise<Write> {
  const basis = await commitFromHash(basisHash, dagWrite);
  const bTreeWrite = new BTreeWrite(dagWrite, formatVersion, basis.valueHash);
  return new Write(
    dagWrite,
    bTreeWrite,
    basis,
    {basisHash, type: MetaType.SnapshotDD31, lastMutationIDs, cookieJSON},
    readIndexesForWrite(basis, dagWrite, formatVersion),
    clientID,
    formatVersion,
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
  dagWrite: DagWrite,
  formatVersion: FormatVersion,
): Map<string, IndexWrite> {
  const m = new Map();
  for (const index of commit.indexes) {
    m.set(
      index.definition.name,
      new IndexWrite(
        index,
        new BTreeWrite(dagWrite, formatVersion, index.valueHash),
      ),
    );
  }
  return m;
}

export async function createIndexBTree(
  lc: LogContext,
  dagWrite: DagWrite,
  valueMap: BTreeRead,
  prefix: string,
  jsonPointer: string,
  allowEmpty: boolean,
  formatVersion: FormatVersion,
): Promise<BTreeWrite> {
  const indexMap = new BTreeWrite(dagWrite, formatVersion);
  for await (const entry of valueMap.scan(prefix)) {
    const key = entry[0];
    if (!key.startsWith(prefix)) {
      break;
    }
    await indexValue(
      lc,
      indexMap,
      IndexOperation.Add,
      key,
      entry[1],
      jsonPointer,
      allowEmpty,
    );
  }
  return indexMap;
}
