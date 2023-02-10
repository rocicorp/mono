import type {LogContext} from '@rocicorp/logger';
import type * as dag from '../dag/mod';
import * as btree from '../btree/mod';
import type {ReadonlyJSONValue} from '../json';
import {
  Commit,
  Meta as CommitMeta,
  IndexDefinition,
  IndexRecord,
  newIndexChange as commitNewIndexChange,
  newLocal as commitNewLocal,
  newSnapshot as commitNewSnapshot,
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
import type {DiffsMap} from '../sync/mod.js';

type IndexChangeMeta = {
  type: MetaType.IndexChange;
  lastMutationID: number;
};

type LocalMeta = {
  type: MetaType.Local;
  mutatorName: string;
  mutatorArgs: ReadonlyJSONValue;
  mutationID: number;
  originalHash: Hash | null;
  timestamp: number;
};

type SnapshotMeta = {
  type: MetaType.Snapshot;
  lastMutationID: number;
  cookie: ReadonlyJSONValue;
};

type Meta = SnapshotMeta | LocalMeta | IndexChangeMeta;

const enum MetaType {
  IndexChange,
  Local,
  Snapshot,
}

export class Write extends Read {
  private readonly _dagWrite: dag.Write;
  private readonly _basis: Commit<CommitMeta> | undefined;
  private readonly _meta: Meta;

  shouldDeepClone = true;

  declare map: BTreeWrite;

  declare readonly indexes: Map<string, IndexWrite>;

  constructor(
    dagWrite: dag.Write,
    map: BTreeWrite,
    basis: Commit<CommitMeta> | undefined,
    meta: Meta,
    indexes: Map<string, IndexWrite>,
  ) {
    // TypeScript has trouble
    super(dagWrite, map, indexes);
    this._dagWrite = dagWrite;
    this._basis = basis;
    this._meta = meta;
  }

  static async newLocal(
    whence: Whence,
    mutatorName: string,
    mutatorArgs: ReadonlyJSONValue,
    originalHash: Hash | null,
    dagWrite: dag.Write,
    timestamp: number,
  ): Promise<Write> {
    const [, basis, bTreeWrite] = await readCommitForBTreeWrite(
      whence,
      dagWrite,
    );
    const mutationID = basis.nextMutationID;
    const indexes = readIndexesForWrite(basis);
    return new Write(
      dagWrite,
      bTreeWrite,
      basis,
      {
        type: MetaType.Local,
        mutatorName,
        mutatorArgs,
        mutationID,
        originalHash,
        timestamp,
      },
      indexes,
    );
  }

  static async newSnapshot(
    whence: Whence,
    mutationID: number,
    cookie: ReadonlyJSONValue,
    dagWrite: dag.Write,
    indexes: Map<string, IndexWrite>,
  ): Promise<Write> {
    const [, basis, bTreeWrite] = await readCommitForBTreeWrite(
      whence,
      dagWrite,
    );
    return new Write(
      dagWrite,
      bTreeWrite,
      basis,
      {type: MetaType.Snapshot, lastMutationID: mutationID, cookie},
      indexes,
    );
  }

  static async newIndexChange(
    whence: Whence,
    dagWrite: dag.Write,
  ): Promise<Write> {
    const [, basis, bTreeWrite] = await readCommitForBTreeWrite(
      whence,
      dagWrite,
    );
    const lastMutationID = basis.mutationID;
    const indexes = readIndexesForWrite(basis);
    return new Write(
      dagWrite,
      bTreeWrite,
      basis,
      {type: MetaType.IndexChange, lastMutationID},
      indexes,
    );
  }

  isRebase(): boolean {
    return (
      this._meta.type === MetaType.Local && this._meta.originalHash !== null
    );
  }

  async put(
    lc: LogContext,
    key: string,
    val: ReadonlyJSONValue,
  ): Promise<void> {
    if (this._meta.type === MetaType.IndexChange) {
      throw new Error('Not allowed');
    }
    const oldVal = lazy(() => this.map.get(key));
    await updateIndexes(lc, this.indexes, this._dagWrite, key, oldVal, val);

    await this.map.put(key, val);
  }

  async del(lc: LogContext, key: string): Promise<boolean> {
    if (this._meta.type === MetaType.IndexChange) {
      throw new Error('Not allowed');
    }

    // TODO(arv): This does the binary search twice. We can do better.
    const oldVal = lazy(() => this.map.get(key));
    if (oldVal !== undefined) {
      await updateIndexes(
        lc,
        this.indexes,
        this._dagWrite,
        key,
        oldVal,
        undefined,
      );
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
    const [hash] = await this.commitWithDiffs(headName, false);
    return hash;
  }

  async commitWithDiffs(
    headName: string,
    generateDiffs: boolean,
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
      basisIndexes = readIndexesForRead(this._basis);
    } else {
      basisIndexes = new Map();
    }

    for (const [name, index] of this.indexes) {
      const valueHash = await index.flush();
      if (generateDiffs) {
        const basisIndex = basisIndexes.get(name);
        const indexDiffResult = await index.withMap(
          this._dagWrite,
          async map => {
            if (basisIndex) {
              return basisIndex.withMap(this._dagWrite, basisMap =>
                btree.diff(basisMap, map),
              );
            }

            // No basis. All keys are new.
            return allEntriesAsDiff(map, 'add');
          },
        );

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
          const indexDiffResult = await basisIndex.withMap(
            this._dagWrite,
            map => allEntriesAsDiff(map, 'del'),
          );
          if (indexDiffResult.length > 0) {
            diffMap.set(name, indexDiffResult);
          }
        }
      }
    }

    const basisHash = this._basis ? this._basis.chunk.hash : null;
    let commit;
    const meta = this._meta;
    switch (meta.type) {
      case MetaType.Local: {
        const {mutationID, mutatorName, mutatorArgs, originalHash, timestamp} =
          meta;
        commit = commitNewLocal(
          this._dagWrite.createChunk,
          basisHash,
          mutationID,
          mutatorName,
          mutatorArgs,
          originalHash,
          valueHash,
          indexRecords,
          timestamp,
        );
        break;
      }
      case MetaType.Snapshot: {
        const {lastMutationID, cookie} = meta;
        commit = commitNewSnapshot(
          this._dagWrite.createChunk,
          basisHash,
          lastMutationID,
          cookie,
          valueHash,
          indexRecords,
        );
        break;
      }
      case MetaType.IndexChange: {
        const {lastMutationID} = meta;
        if (this._basis !== undefined) {
          if (this._basis.mutationID !== lastMutationID) {
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

    await this._dagWrite.commit();

    return [commit.chunk.hash, diffMap];
  }

  close(): void {
    this._dagWrite.close();
  }
}

export async function updateIndexes(
  lc: LogContext,
  indexes: Map<string, IndexWrite>,
  dagWrite: dag.Write,
  key: string,
  oldValGetter: () => Promise<ReadonlyJSONValue | undefined>,
  newVal: ReadonlyJSONValue | undefined,
): Promise<void> {
  const ps: Promise<void>[] = [];
  for (const idx of indexes.values()) {
    if (key.startsWith(idx.meta.definition.keyPrefix)) {
      const oldVal = await oldValGetter();
      await idx.withMap(dagWrite, async map => {
        if (oldVal !== undefined) {
          ps.push(
            indexValue(
              lc,
              map,
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
              map,
              IndexOperation.Add,
              key,
              newVal,
              idx.meta.definition.jsonPointer,
              idx.meta.definition.allowEmpty,
            ),
          );
        }
      });
    }
  }
  await Promise.all(ps);
}

export async function initDB(
  dagWrite: dag.Write,
  headName: string,
): Promise<Hash> {
  const w = new Write(
    dagWrite,
    new BTreeWrite(dagWrite),
    undefined,
    {type: MetaType.Snapshot, lastMutationID: 0, cookie: null},
    new Map(),
  );
  return await w.commit(headName);
}

export function readIndexesForWrite(
  commit: Commit<CommitMeta>,
): Map<string, IndexWrite> {
  const m = new Map();
  for (const index of commit.indexes) {
    m.set(index.definition.name, new IndexWrite(index, undefined));
  }
  return m;
}
