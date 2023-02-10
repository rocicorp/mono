import type * as dag from '../dag/mod';
import {assertJSONValue} from '../json';
import {
  assert,
  assertArray,
  assertBoolean,
  assertNumber,
  assertObject,
  assertString,
} from '../asserts';
import {assertHash, Hash} from '../hash';
import {skipCommitDataAsserts} from '../config.js';
import type {InternalValue} from '../internal-value.js';
import type {ClientID} from '../sync/client-id.js';
import type {MustGetChunk} from '../dag/store.js';

export const DEFAULT_HEAD_NAME = 'main';

// TODO(arv): Add new entries for DD31
export const enum MetaType {
  NONE = 0,
  IndexChange = 1,
  Local = 2,
  Snapshot = 3,
}

export class Commit<M extends Meta> {
  readonly chunk: dag.Chunk<CommitData<M>>;

  constructor(chunk: dag.Chunk<CommitData<M>>) {
    this.chunk = chunk;
  }

  get meta(): M {
    return this.chunk.data.meta;
  }

  isLocal(): this is Commit<LocalMeta> {
    return this.meta.type === MetaType.Local;
  }

  isSnapshot(): this is Commit<SnapshotMeta> {
    return this.meta.type === MetaType.Snapshot;
  }

  isIndexChange(): this is Commit<IndexChangeMeta> {
    return this.meta.type === MetaType.IndexChange;
  }

  get valueHash(): Hash {
    // Already validated!
    return this.chunk.data.valueHash;
  }

  async getMutationID(
    clientID: ClientID,
    dagRead: dag.MustGetChunk,
  ): Promise<number> {
    const {meta} = this;
    switch (meta.type) {
      case MetaType.IndexChange:
        if (DD31) {
          const {basisHash} = meta;
          const basisCommit = await fromHash(basisHash, dagRead);
          return basisCommit.getMutationID(clientID, dagRead);
        } else {
          return meta.lastMutationID;
        }
      case MetaType.Snapshot: {
        if (DD31) {
          assertSnapshotMetaDD31(meta);
          const lmid = meta.lastMutationIDs[clientID];
          if (lmid === undefined) {
            // TODO(DD31, arv): Is this the right behavior?
            // Not in the base snap shot. This is a new client.
            return 0;
          }
          return lmid;
        }
        assertSnapshotMeta(meta);
        return meta.lastMutationID;
      }
      case MetaType.Local: {
        if (DD31) {
          assertLocalMetaDD31(meta);
          if (meta.clientID === clientID) {
            return meta.mutationID;
          }
          const {basisHash} = meta;
          const basisCommit = await fromHash(basisHash, dagRead);
          return basisCommit.getMutationID(clientID, dagRead);
        }
        return meta.mutationID;
      }
    }
  }

  async getNextMutationID(
    clientID: ClientID,
    dagRead: dag.MustGetChunk,
  ): Promise<number> {
    return (await this.getMutationID(clientID, dagRead)) + 1;
  }

  get indexes(): readonly IndexRecord[] {
    // Already validated!
    return this.chunk.data.indexes;
  }
}

/**
 * Returns the set of local commits from the given `fromCommitHash` back to but not
 * including its base snapshot. If `fromCommitHash` is a snapshot, the returned vector
 * will be empty. When, as typical, `fromCommitHash` is the head of the default chain
 * then the returned commits are the set of pending commits, ie the set of local commits
 * that have not yet been pushed to the data layer.
 *
 * The vector of commits is returned in reverse chain order, that is, starting
 * with the commit with hash `fromCommitHash` and walking backwards.
 */
export async function localMutations(
  fromCommitHash: Hash,
  dagRead: dag.Read,
): Promise<Commit<LocalMeta>[]> {
  const commits = await chain(fromCommitHash, dagRead);
  // Filter does not deal with type narrowing.
  return commits.filter(c => c.isLocal()) as Commit<LocalMeta>[];
}

export async function localMutationsGreaterThan(
  fromCommitHash: Hash,
  mutationIDLimits: Record<ClientID, number>,
  dagRead: dag.Read,
): Promise<Commit<LocalMeta>[]> {
  if (DD31) {
    let commit = await fromHash(fromCommitHash, dagRead);
    const commits: Commit<LocalMeta>[] = [];
    const remainingMutationIDLimits = {...mutationIDLimits};
    while (
      !commit.isSnapshot() &&
      Object.keys(remainingMutationIDLimits).length > 0
    ) {
      if (commit.isLocal()) {
        const {meta} = commit;
        const {mutationID} = meta;
        assertLocalMetaDD31(meta);
        const mutationIDLowerLimit = remainingMutationIDLimits[meta.clientID];
        if (mutationIDLowerLimit !== undefined) {
          if (mutationID <= mutationIDLowerLimit) {
            delete remainingMutationIDLimits[meta.clientID];
          } else {
            commits.push(commit);
          }
        }
      }
      const {basisHash} = commit.meta;
      if (basisHash === null) {
        throw new Error(`Commit ${commit.chunk.hash} has no basis`);
      }
      commit = await fromHash(basisHash, dagRead);
    }
    return commits;
  } else {
    throw new Error('localMutationsGreaterThan should only be called in DD31');
  }
}

export async function baseSnapshot(
  hash: Hash,
  dagRead: dag.Read,
): Promise<Commit<SnapshotMeta | SnapshotMetaDD31>> {
  let commit = await fromHash(hash, dagRead);
  while (!commit.isSnapshot()) {
    const {meta} = commit;
    const {basisHash} = meta;
    if (basisHash === null) {
      throw new Error(`Commit ${commit.chunk.hash} has no basis`);
    }
    commit = await fromHash(basisHash, dagRead);
  }
  return commit;
}

export function snapshotMetaParts(
  c: Commit<SnapshotMeta | SnapshotMetaDD31>,
  clientID: ClientID,
): [lastMutationID: number, cookie: InternalValue] {
  const m = c.meta;
  let lmid;
  if (DD31) {
    assertSnapshotMetaDD31(m);
    lmid = m.lastMutationIDs[clientID];
    assertNumber(lmid);
    return [lmid, m.cookieJSON];
  }

  assertSnapshotMeta(m);
  return [m.lastMutationID, m.cookieJSON];
}

/**
 * Returns all commits from the commit with from_commit_hash to its base
 * snapshot, inclusive of both. Resulting vector is in chain-head-first order
 * (so snapshot comes last).
 */
export async function chain(
  fromCommitHash: Hash,
  dagRead: dag.Read,
): Promise<Commit<Meta>[]> {
  let commit = await fromHash(fromCommitHash, dagRead);
  const commits = [];
  while (!commit.isSnapshot()) {
    const {meta} = commit;
    const {basisHash} = meta;
    if (basisHash === null) {
      throw new Error(`Commit ${commit.chunk.hash} has no basis`);
    }
    commits.push(commit);
    commit = await fromHash(basisHash, dagRead);
  }
  commits.push(commit);
  return commits;
}

export async function fromHash(
  hash: Hash,
  dagRead: MustGetChunk,
): Promise<Commit<Meta>> {
  const chunk = await dagRead.mustGetChunk(hash);
  return fromChunk(chunk);
}

export async function fromHead(
  name: string,
  dagRead: dag.Read,
): Promise<Commit<Meta>> {
  const hash = await dagRead.getHead(name);
  assert(hash, `Missing head ${name}`);
  return fromHash(hash, dagRead);
}

export type IndexChangeMeta = {
  readonly type: MetaType.IndexChange;
  readonly basisHash: Hash;
  readonly lastMutationID: number;
};

function assertIndexChangeMeta(
  v: Record<string, unknown>,
): asserts v is IndexChangeMeta {
  // type already asserted
  assertNumber(v.lastMutationID);

  // Note: indexes are already validated for all commit types. Only additional
  // things to validate are:
  //   - last_mutation_id is equal to the basis
  //   - value_hash has not been changed
  // However we don't have a write transaction this deep, so these validated at
  // commit time.
}

export type LocalMeta = {
  readonly type: MetaType.Local;
  readonly basisHash: Hash;
  readonly mutationID: number;
  readonly mutatorName: string;
  readonly mutatorArgsJSON: InternalValue;
  readonly originalHash: Hash | null;
  readonly timestamp: number;
};

export type LocalMetaDD31 = LocalMeta & {
  readonly clientID: ClientID;
};

function assertLocalMeta(v: Record<string, unknown>): asserts v is LocalMeta {
  // type already asserted
  assertNumber(v.mutationID);
  assertString(v.mutatorName);
  if (!v.mutatorName) {
    throw new Error('Missing mutator name');
  }
  assertJSONValue(v.mutatorArgsJSON);
  if (v.originalHash !== null) {
    assertHash(v.originalHash);
  }
  assertNumber(v.timestamp);
}

export function assertLocalMetaDD31(
  v: Record<string, unknown>,
): asserts v is LocalMetaDD31 {
  // type already asserted
  assertNumber(v.mutationID);
  assertString(v.mutatorName);
  if (!v.mutatorName) {
    throw new Error('Missing mutator name');
  }
  assertJSONValue(v.mutatorArgsJSON);
  if (v.originalHash !== null) {
    assertHash(v.originalHash);
  }
  assertNumber(v.timestamp);
  assertString(v.clientID);
}

export function isLocalMetaDD31(
  meta: LocalMeta | LocalMetaDD31,
): meta is LocalMetaDD31 {
  return DD31 && (meta as Partial<LocalMetaDD31>).clientID !== undefined;
}

export type SnapshotMeta = {
  readonly type: MetaType.Snapshot;
  readonly basisHash: Hash | null;
  readonly lastMutationID: number;
  readonly cookieJSON: InternalValue;
};

export type SnapshotMetaDD31 = {
  readonly type: MetaType.Snapshot;
  readonly basisHash: Hash | null;
  readonly lastMutationIDs: Record<ClientID, number>;
  readonly cookieJSON: InternalValue;
};

export function assertSnapshotMeta(
  v: Record<string, unknown>,
): asserts v is SnapshotMeta {
  assert(!DD31);
  // type already asserted
  assertNumber(v.lastMutationID);
  assertJSONValue(v.cookieJSON);
}

export type Meta =
  | IndexChangeMeta
  | LocalMeta
  | LocalMetaDD31
  | SnapshotMeta
  | SnapshotMetaDD31;

export function assertSnapshotMetaDD31(
  v: Record<string, unknown>,
): asserts v is SnapshotMetaDD31 {
  assert(DD31);
  // type already asserted
  assertObject(v.lastMutationIDs);
  for (const lmid of Object.values(v.lastMutationIDs)) {
    assertNumber(lmid);
  }
  assertJSONValue(v.cookieJSON);
}

export function isSnapshotMetaDD31(
  meta: SnapshotMeta | SnapshotMetaDD31,
): meta is SnapshotMetaDD31 {
  return (
    DD31 && (meta as Partial<SnapshotMetaDD31>).lastMutationIDs !== undefined
  );
}

function assertMeta(v: unknown): asserts v is Meta {
  assertObject(v);
  if (v.basisHash !== null) {
    assertString(v.basisHash);
  }

  assertNumber(v.type);
  switch (v.type) {
    case MetaType.IndexChange:
      assertIndexChangeMeta(v);
      break;
    case MetaType.Local:
      if (DD31) {
        assertLocalMetaDD31(v);
      } else {
        assertLocalMeta(v);
      }
      break;
    case MetaType.Snapshot:
      if (DD31) {
        assertSnapshotMetaDD31(v);
      } else {
        assertSnapshotMeta(v);
      }
      break;
    default:
      throw new Error(`Invalid enum value ${v.type}`);
  }
}

export type IndexDefinition = {
  readonly name: string;
  // keyPrefix describes a subset of the primary key to index
  readonly keyPrefix: string;
  // jsonPointer describes the (sub-)value to index (secondary index)
  readonly jsonPointer: string;

  readonly allowEmpty?: boolean;
};

function assertIndexDefinition(v: unknown): asserts v is IndexDefinition {
  assertObject(v);
  assertString(v.name);
  assertString(v.keyPrefix);
  assertString(v.jsonPointer);

  if (v.allowEmpty !== undefined) {
    assertBoolean(v.allowEmpty);
  }
}

export type IndexRecord = {
  readonly definition: IndexDefinition;
  readonly valueHash: Hash;
};

function assertIndexRecord(v: unknown): asserts v is IndexRecord {
  assertObject(v);
  assertIndexDefinition(v.definition);
  assertString(v.valueHash);
}

export function newLocal(
  createChunk: dag.CreateChunk,
  basisHash: Hash,
  mutationID: number,
  mutatorName: string,
  mutatorArgsJSON: InternalValue,
  originalHash: Hash | null,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
  timestamp: number,
  clientID: ClientID,
): Commit<LocalMeta | LocalMetaDD31> {
  if (DD31) {
    const meta: LocalMetaDD31 = {
      type: MetaType.Local,
      basisHash,
      mutationID,
      mutatorName,
      mutatorArgsJSON,
      originalHash,
      timestamp,
      clientID,
    };
    return commitFromCommitData(createChunk, {meta, valueHash, indexes});
  }
  const meta: LocalMeta = {
    type: MetaType.Local,
    basisHash,
    mutationID,
    mutatorName,
    mutatorArgsJSON,
    originalHash,
    timestamp,
  };
  return commitFromCommitData(createChunk, {meta, valueHash, indexes});
}

export function newSnapshot(
  createChunk: dag.CreateChunk,
  basisHash: Hash | null,
  lastMutationID: number,
  cookieJSON: InternalValue,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
): Commit<SnapshotMeta> {
  return commitFromCommitData(
    createChunk,
    newSnapshotCommitData(
      basisHash,
      lastMutationID,
      cookieJSON,
      valueHash,
      indexes,
    ),
  );
}

export function newSnapshotDD31(
  createChunk: dag.CreateChunk,
  basisHash: Hash | null,
  lastMutationIDs: Record<ClientID, number>,
  cookieJSON: InternalValue,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
): Commit<SnapshotMetaDD31> {
  return commitFromCommitData(
    createChunk,
    newSnapshotCommitDataDD31(
      basisHash,
      lastMutationIDs,
      cookieJSON,
      valueHash,
      indexes,
    ),
  );
}

export function newSnapshotCommitData(
  basisHash: Hash | null,
  lastMutationID: number,
  cookieJSON: InternalValue,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
): CommitData<SnapshotMeta> {
  assert(!DD31);
  const meta: SnapshotMeta = {
    type: MetaType.Snapshot,
    basisHash,
    lastMutationID,
    cookieJSON,
  };
  return {meta, valueHash, indexes};
}

export function newSnapshotCommitDataDD31(
  basisHash: Hash | null,
  lastMutationIDs: Record<ClientID, number>,
  cookieJSON: InternalValue,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
): CommitData<SnapshotMetaDD31> {
  assert(DD31);
  const meta: SnapshotMetaDD31 = {
    type: MetaType.Snapshot,
    basisHash,
    lastMutationIDs,
    cookieJSON,
  };
  return {meta, valueHash, indexes};
}

export function newIndexChange(
  createChunk: dag.CreateChunk,
  basisHash: Hash,
  lastMutationID: number,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
): Commit<IndexChangeMeta> {
  const meta: IndexChangeMeta = {
    type: MetaType.IndexChange,
    basisHash,
    lastMutationID,
  };
  return commitFromCommitData(createChunk, {meta, valueHash, indexes});
}

export function fromChunk(chunk: dag.Chunk<unknown>): Commit<Meta> {
  validateChunk(chunk);
  return new Commit(chunk);
}

function commitFromCommitData<M extends Meta>(
  createChunk: dag.CreateChunk,
  data: CommitData<M>,
): Commit<M> {
  return new Commit(createChunk(data, getRefs(data)));
}

export function getRefs(data: CommitData<Meta>): Hash[] {
  const refs: Hash[] = [data.valueHash];
  const {meta} = data;
  switch (meta.type) {
    case MetaType.IndexChange:
      meta.basisHash && refs.push(meta.basisHash);
      break;
    case MetaType.Local:
      meta.basisHash && refs.push(meta.basisHash);
      // Local has weak originalHash
      break;
    case MetaType.Snapshot:
      // Snapshot has weak basisHash
      break;
  }

  for (const index of data.indexes) {
    refs.push(index.valueHash);
  }

  return refs;
}

export type CommitData<M extends Meta> = {
  readonly meta: M;
  readonly valueHash: Hash;
  readonly indexes: readonly IndexRecord[];
};

export function assertCommitData(v: unknown): asserts v is CommitData<Meta> {
  if (skipCommitDataAsserts) {
    return;
  }

  assertObject(v);
  assertMeta(v.meta);
  assertString(v.valueHash);
  assertArray(v.indexes);
  for (const index of v.indexes) {
    assertIndexRecord(index);
  }
}

function validateChunk(
  chunk: dag.Chunk<unknown>,
): asserts chunk is dag.Chunk<CommitData<Meta>> {
  const {data} = chunk;
  assertCommitData(data);

  // Indexes is optional
  const seen = new Set();
  for (const index of data.indexes) {
    const {name} = index.definition;
    if (seen.has(name)) {
      throw new Error(`Duplicate index ${name}`);
    }
    seen.add(name);
  }
}
