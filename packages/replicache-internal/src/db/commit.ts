import type * as dag from '../dag/mod';
import type {ReadonlyJSONValue} from '../json';
import {assertJSONValue} from '../json';
import {
  assert,
  assertArray,
  assertBoolean,
  assertNumber,
  assertObject,
  assertString,
} from '../asserts';
import type {Value} from '../kv/store';
import {assertHash, Hash} from '../hash';
import {skipCommitDataAsserts} from '../config.js';

export const DEFAULT_HEAD_NAME = 'main';

export const enum MetaTyped {
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
    return this.meta.type === MetaTyped.Local;
  }

  isSnapshot(): this is Commit<SnapshotMeta> {
    return this.meta.type === MetaTyped.Snapshot;
  }

  isIndexChange(): this is Commit<IndexChangeMeta> {
    return this.meta.type === MetaTyped.IndexChange;
  }

  get valueHash(): Hash {
    // Already validated!
    return this.chunk.data.valueHash;
  }

  get mutationID(): number {
    const {meta} = this;
    switch (meta.type) {
      case MetaTyped.IndexChange:
      case MetaTyped.Snapshot:
        return meta.lastMutationID;
      case MetaTyped.Local:
        return meta.mutationID;
    }
  }

  get nextMutationID(): number {
    return this.mutationID + 1;
  }

  get indexes(): readonly IndexRecord[] {
    // Already validated!
    return this.chunk.data.indexes;
  }
}

/**
 * Returns the set of local commits from the given from_commit_hash back to but not
 * including its base snapshot. If from_commit_hash is a snapshot, the returned vector
 * will be empty. When, as typical, from_commit_hash is the head of the default chain
 * then the returned commits are the set of pending commits, ie the set of local commits
 * that have not yet been pushed to the data layer.
 *
 * The vector of commits is returned in reverse chain order, that is, starting
 * with the commit with hash from_commit_hash and walking backwards.
 */
export async function localMutations(
  fromCommitHash: Hash,
  dagRead: dag.Read,
): Promise<Commit<LocalMeta>[]> {
  const commits = await chain(fromCommitHash, dagRead);
  // Filter does not deal with type narrowing.
  return commits.filter(c => c.isLocal()) as Commit<LocalMeta>[];
}

export async function baseSnapshot(
  hash: Hash,
  dagRead: dag.Read,
): Promise<Commit<SnapshotMeta>> {
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
  c: Commit<SnapshotMeta>,
): [lastMutationID: number, cookie: ReadonlyJSONValue] {
  const m = c.meta;
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
  dagRead: dag.Read,
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

type BasisHash = {
  readonly basisHash: Hash | null;
};

export type IndexChangeMeta = BasisHash & {
  readonly type: MetaTyped.IndexChange;
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

export type LocalMeta = BasisHash & {
  readonly type: MetaTyped.Local;
  readonly mutationID: number;
  readonly mutatorName: string;
  readonly mutatorArgsJSON: ReadonlyJSONValue;
  readonly originalHash: Hash | null;
  readonly timestamp: number;
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

export type SnapshotMeta = BasisHash & {
  readonly type: MetaTyped.Snapshot;
  readonly lastMutationID: number;
  readonly cookieJSON: ReadonlyJSONValue;
};

function assertSnapshotMeta(
  v: Record<string, unknown>,
): asserts v is SnapshotMeta {
  // type already asserted
  assertNumber(v.lastMutationID);
  assertJSONValue(v.cookieJSON);
}

export type Meta = IndexChangeMeta | LocalMeta | SnapshotMeta;

function assertMeta(v: unknown): asserts v is Meta {
  assertObject(v);
  if (v.basisHash !== null) {
    assertString(v.basisHash);
  }

  assertNumber(v.type);
  switch (v.type) {
    case MetaTyped.IndexChange:
      assertIndexChangeMeta(v);
      break;
    case MetaTyped.Local:
      assertLocalMeta(v);
      break;
    case MetaTyped.Snapshot:
      assertSnapshotMeta(v);
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
  basisHash: Hash | null,
  mutationID: number,
  mutatorName: string,
  mutatorArgsJSON: ReadonlyJSONValue,
  originalHash: Hash | null,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
  timestamp: number,
): Commit<LocalMeta> {
  const meta: LocalMeta = {
    type: MetaTyped.Local,
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
  cookieJSON: ReadonlyJSONValue,
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

export function newSnapshotCommitData(
  basisHash: Hash | null,
  lastMutationID: number,
  cookieJSON: ReadonlyJSONValue,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
): CommitData<SnapshotMeta> {
  const meta: SnapshotMeta = {
    type: MetaTyped.Snapshot,
    basisHash,
    lastMutationID,
    cookieJSON,
  };
  return {meta, valueHash, indexes};
}

export function newIndexChange(
  createChunk: dag.CreateChunk,
  basisHash: Hash | null,
  lastMutationID: number,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
): Commit<IndexChangeMeta> {
  const meta: IndexChangeMeta = {
    type: MetaTyped.IndexChange,
    basisHash,
    lastMutationID,
  };
  return commitFromCommitData(createChunk, {meta, valueHash, indexes});
}

export function fromChunk(chunk: dag.Chunk): Commit<Meta> {
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
    case MetaTyped.IndexChange:
      meta.basisHash && refs.push(meta.basisHash);
      break;
    case MetaTyped.Local:
      meta.basisHash && refs.push(meta.basisHash);
      // Local has weak originalHash
      break;
    case MetaTyped.Snapshot:
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
  chunk: dag.Chunk<Value>,
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

export function getMetaTypedDisplayName(type: MetaTyped): string {
  let displayName = undefined;
  switch (type) {
    case MetaTyped.IndexChange:
      displayName = 'IndexChange';
      break;
    case MetaTyped.Local:
      displayName = 'Local';
      break;
    case MetaTyped.Snapshot:
      displayName = 'Snapshot';
      break;
  }
  if (displayName === undefined) {
    throw new Error(`MetaTyped display name not found for type ${type}`);
  }
  return displayName;
}
