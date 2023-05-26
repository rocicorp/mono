import {
  assert,
  assertArray,
  assertBoolean,
  assertNumber,
  assertObject,
  assertString,
  unreachable,
} from 'shared/asserts.js';
import {skipCommitDataAsserts} from '../config.js';
import {FrozenCookie, compareCookies} from '../cookies.js';
import type * as dag from '../dag/mod.js';
import {MustGetChunk, mustGetHeadHash} from '../dag/store.js';
import {Hash, assertHash} from '../hash.js';
import type {IndexDefinition} from '../index-defs.js';
import {
  FrozenJSONValue,
  FrozenTag,
  assertDeepFrozen,
  assertJSONValue,
  deepFreeze,
} from '../json.js';
import type {ClientID} from '../sync/ids.js';

export const DEFAULT_HEAD_NAME = 'main';

export const enum MetaType {
  IndexChangeSDD = 1,
  LocalSDD = 2,
  SnapshotSDD = 3,
  LocalDD31 = 4,
  SnapshotDD31 = 5,
}

export function commitIsLocalSDD(
  commit: Commit<Meta>,
): commit is Commit<LocalMetaSDD> {
  return isLocalMetaSDD(commit.meta);
}

export function commitIsLocalDD31(
  commit: Commit<Meta>,
): commit is Commit<LocalMetaDD31> {
  return isLocalMetaDD31(commit.meta);
}

export function commitIsLocal(
  commit: Commit<Meta>,
): commit is Commit<LocalMetaDD31 | LocalMetaSDD> {
  return commitIsLocalDD31(commit) || commitIsLocalSDD(commit);
}

function commitIsSnapshotDD31(
  commit: Commit<Meta>,
): commit is Commit<SnapshotMetaDD31> {
  return isSnapshotMetaDD31(commit.meta);
}

function commitIsSnapshotSDD(
  commit: Commit<Meta>,
): commit is Commit<SnapshotMetaSDD> {
  return isSnapshotMetaSDD(commit.meta);
}

export function commitIsSnapshot(
  commit: Commit<Meta>,
): commit is Commit<SnapshotMetaDD31 | SnapshotMetaSDD> {
  return commitIsSnapshotDD31(commit) || commitIsSnapshotSDD(commit);
}

export class Commit<M extends Meta> {
  readonly chunk: dag.Chunk<CommitData<M>>;

  constructor(chunk: dag.Chunk<CommitData<M>>) {
    this.chunk = chunk;
  }

  get meta(): M {
    return this.chunk.data.meta;
  }

  get valueHash(): Hash {
    // Already validated!
    return this.chunk.data.valueHash;
  }

  getMutationID(
    clientID: ClientID,
    dagRead: dag.MustGetChunk,
  ): Promise<number> {
    return getMutationID(clientID, dagRead, this.meta);
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

export async function getMutationID(
  clientID: ClientID,
  dagRead: dag.MustGetChunk,
  meta: Meta,
): Promise<number> {
  switch (meta.type) {
    case MetaType.IndexChangeSDD:
      return meta.lastMutationID;

    case MetaType.SnapshotSDD:
      return meta.lastMutationID;

    case MetaType.SnapshotDD31:
      return meta.lastMutationIDs[clientID] ?? 0;

    case MetaType.LocalSDD:
      return meta.mutationID;

    case MetaType.LocalDD31: {
      if (meta.clientID === clientID) {
        return meta.mutationID;
      }
      const {basisHash} = meta;
      const basisCommit = await commitFromHash(basisHash, dagRead);
      return getMutationID(clientID, dagRead, basisCommit.meta);
    }

    default:
      unreachable();
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
): Promise<Commit<LocalMetaSDD | LocalMetaDD31>[]> {
  const commits = await commitChain(fromCommitHash, dagRead);
  // Filter does not deal with type narrowing.
  return commits.filter(c => commitIsLocal(c)) as Commit<
    LocalMetaSDD | LocalMetaDD31
  >[];
}

export async function localMutationsDD31(
  fromCommitHash: Hash,
  dagRead: dag.Read,
): Promise<Commit<LocalMetaDD31>[]> {
  const commits = await commitChain(fromCommitHash, dagRead);
  // Filter does not deal with type narrowing.
  return commits.filter(c => commitIsLocalDD31(c)) as Commit<LocalMetaDD31>[];
}

export async function localMutationsGreaterThan(
  commit: Commit<Meta>,
  mutationIDLimits: Record<ClientID, number>,
  dagRead: dag.Read,
): Promise<Commit<LocalMetaDD31>[]> {
  const commits: Commit<LocalMetaDD31>[] = [];
  const remainingMutationIDLimits = new Map(Object.entries(mutationIDLimits));
  while (!commitIsSnapshot(commit) && remainingMutationIDLimits.size > 0) {
    if (commitIsLocalDD31(commit)) {
      const {meta} = commit;
      const mutationIDLowerLimit = remainingMutationIDLimits.get(meta.clientID);
      if (mutationIDLowerLimit !== undefined) {
        if (meta.mutationID <= mutationIDLowerLimit) {
          remainingMutationIDLimits.delete(meta.clientID);
        } else {
          commits.push(commit as Commit<LocalMetaDD31>);
        }
      }
    }
    const {basisHash} = commit.meta;
    if (basisHash === null) {
      throw new Error(`Commit ${commit.chunk.hash} has no basis`);
    }
    commit = await commitFromHash(basisHash, dagRead);
  }
  return commits;
}

export async function baseSnapshotFromHead(
  name: string,
  dagRead: dag.Read,
): Promise<Commit<SnapshotMetaSDD | SnapshotMetaDD31>> {
  const hash = await dagRead.getHead(name);
  assert(hash, `Missing head ${name}`);
  return baseSnapshotFromHash(hash, dagRead);
}

export async function baseSnapshotHashFromHash(
  hash: Hash,
  dagRead: dag.Read,
): Promise<Hash> {
  return (await baseSnapshotFromHash(hash, dagRead)).chunk.hash;
}

export async function baseSnapshotFromHash(
  hash: Hash,
  dagRead: dag.Read,
): Promise<Commit<SnapshotMetaSDD | SnapshotMetaDD31>> {
  const commit = await commitFromHash(hash, dagRead);
  return baseSnapshotFromCommit(commit, dagRead);
}

export async function baseSnapshotFromCommit(
  commit: Commit<Meta>,
  dagRead: dag.Read,
): Promise<Commit<SnapshotMetaSDD | SnapshotMetaDD31>> {
  while (!commitIsSnapshot(commit)) {
    const {meta} = commit;
    if (isLocalMetaDD31(meta)) {
      commit = await commitFromHash(meta.baseSnapshotHash, dagRead);
    } else {
      const {basisHash} = meta;
      if (basisHash === null) {
        throw new Error(`Commit ${commit.chunk.hash} has no basis`);
      }
      commit = await commitFromHash(basisHash, dagRead);
    }
  }
  return commit;
}

export function snapshotMetaParts(
  c: Commit<SnapshotMetaSDD | SnapshotMetaDD31>,
  clientID: ClientID,
): [lastMutationID: number, cookie: FrozenCookie | FrozenJSONValue] {
  const m = c.meta;
  if (isSnapshotMetaDD31(m)) {
    const lmid = m.lastMutationIDs[clientID] ?? 0;
    return [lmid, m.cookieJSON];
  }
  return [m.lastMutationID, m.cookieJSON];
}

export function compareCookiesForSnapshots(
  a: Commit<SnapshotMetaDD31>,
  b: Commit<SnapshotMetaDD31>,
): number {
  return compareCookies(a.meta.cookieJSON, b.meta.cookieJSON);
}

/**
 * Returns all commits from the commit with fromCommitHash to its base snapshot,
 * inclusive of both. Resulting vector is in chain-head-first order (so snapshot
 * comes last).
 */
export async function commitChain(
  fromCommitHash: Hash,
  dagRead: dag.Read,
): Promise<Commit<Meta>[]> {
  let commit = await commitFromHash(fromCommitHash, dagRead);
  const commits = [];
  while (!commitIsSnapshot(commit)) {
    const {meta} = commit;
    const {basisHash} = meta;
    if (basisHash === null) {
      throw new Error(`Commit ${commit.chunk.hash} has no basis`);
    }
    commits.push(commit);
    commit = await commitFromHash(basisHash, dagRead);
  }
  commits.push(commit);
  return commits;
}

export async function commitFromHash(
  hash: Hash,
  dagRead: MustGetChunk,
): Promise<Commit<Meta>> {
  const chunk = await dagRead.mustGetChunk(hash);
  return fromChunk(chunk);
}

export async function commitFromHead(
  name: string,
  dagRead: dag.Read,
): Promise<Commit<Meta>> {
  const hash = await mustGetHeadHash(name, dagRead);
  return commitFromHash(hash, dagRead);
}

export type IndexChangeMetaSDD = {
  readonly type: MetaType.IndexChangeSDD;
  readonly basisHash: Hash;
  readonly lastMutationID: number;
};

function assertIndexChangeMeta(
  v: Record<string, unknown>,
): asserts v is IndexChangeMetaSDD {
  // type already asserted
  assertNumber(v.lastMutationID);

  // Note: indexes are already validated for all commit types. Only additional
  // things to validate are:
  //   - lastMutationID is equal to the basis
  //   - valueHash has not been changed
  // However we don't have a write transaction this deep, so these validated at
  // commit time.
}

export function assertIndexChangeCommit(
  c: Commit<Meta>,
): asserts c is Commit<IndexChangeMetaSDD> {
  assertIndexChangeMeta(c.meta);
}

export type LocalMetaSDD = {
  readonly type: MetaType.LocalSDD;
  readonly basisHash: Hash;
  readonly mutationID: number;
  readonly mutatorName: string;
  readonly mutatorArgsJSON: FrozenJSONValue;
  readonly originalHash: Hash | null;
  readonly timestamp: number;
};

export type LocalMetaDD31 = Omit<LocalMetaSDD, 'type'> & {
  readonly type: MetaType.LocalDD31;
  readonly clientID: ClientID;
  readonly baseSnapshotHash: Hash;
};

export type LocalMeta = LocalMetaSDD | LocalMetaDD31;

function assertLocalMetaSDD(
  v: Record<string, unknown>,
): asserts v is LocalMetaSDD {
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
  assertString(v.clientID);
  assertLocalMetaSDD(v);
}

export function isLocalMetaDD31(meta: Meta): meta is LocalMetaDD31 {
  return meta.type === MetaType.LocalDD31;
}

function isLocalMetaSDD(meta: Meta): meta is LocalMetaSDD {
  return meta.type === MetaType.LocalSDD;
}

export function assertLocalCommitDD31(
  c: Commit<Meta>,
): asserts c is Commit<LocalMetaDD31> {
  assertLocalMetaDD31(c.meta);
}

export function assertLocalCommitSDD(
  c: Commit<Meta>,
): asserts c is Commit<LocalMetaSDD> {
  assertLocalMetaSDD(c.meta);
}

export type SnapshotMetaSDD = {
  readonly type: MetaType.SnapshotSDD;
  readonly basisHash: Hash | null;
  readonly lastMutationID: number;
  readonly cookieJSON: FrozenJSONValue;
};

export type SnapshotMetaDD31 = {
  readonly type: MetaType.SnapshotDD31;
  readonly basisHash: Hash | null;
  readonly lastMutationIDs: Record<ClientID, number>;
  readonly cookieJSON: FrozenCookie;
};

export type SnapshotMeta = SnapshotMetaSDD | SnapshotMetaDD31;

function assertSnapshotMetaBase(v: Record<string, unknown>) {
  // type already asserted
  if (v.basisHash !== null) {
    assertHash(v.basisHash);
  }
  assertJSONValue(v.cookieJSON);
}

export function assertSnapshotMetaSDD(
  v: Record<string, unknown>,
): asserts v is SnapshotMetaSDD {
  assertSnapshotMetaBase(v);
  assertNumber(v.lastMutationID);
}

export function assertSnapshotMetaDD31(
  v: Record<string, unknown>,
): asserts v is SnapshotMetaDD31 {
  assertSnapshotMetaBase(v);
  assertLastMutationIDs(v.lastMutationIDs);
}

function assertLastMutationIDs(
  v: unknown,
): asserts v is Record<ClientID, number> {
  assertObject(v);
  for (const e of Object.values(v)) {
    assertNumber(e);
  }
}

export function assertSnapshotCommitSDD(
  c: Commit<Meta>,
): asserts c is Commit<SnapshotMetaSDD> {
  assertSnapshotMetaSDD(c.meta);
}

export type Meta =
  | IndexChangeMetaSDD
  | LocalMetaSDD
  | LocalMetaDD31
  | SnapshotMetaSDD
  | SnapshotMetaDD31;

export function assertSnapshotCommitDD31(
  c: Commit<Meta>,
): asserts c is Commit<SnapshotMetaDD31> {
  assertSnapshotMetaDD31(c.meta);
}

function isSnapshotMetaDD31(meta: Meta): meta is SnapshotMetaDD31 {
  return meta.type === MetaType.SnapshotDD31;
}

function isSnapshotMetaSDD(meta: Meta): meta is SnapshotMetaSDD {
  return meta.type === MetaType.SnapshotSDD;
}

function assertMeta(v: unknown): asserts v is Meta {
  assertObject(v);
  assertDeepFrozen(v);
  if (v.basisHash !== null) {
    assertString(v.basisHash);
  }

  assertNumber(v.type);
  switch (v.type) {
    case MetaType.IndexChangeSDD:
      assertIndexChangeMeta(v);
      break;
    case MetaType.LocalSDD:
      assertLocalMetaSDD(v);
      break;
    case MetaType.LocalDD31:
      assertLocalMetaDD31(v);
      break;
    case MetaType.SnapshotSDD:
      assertSnapshotMetaSDD(v);
      break;
    case MetaType.SnapshotDD31:
      assertSnapshotMetaDD31(v);
      break;
    default:
      throw new Error(`Invalid enum value ${v.type}`);
  }
}

/**
 * This is the type used for index definitions as defined in the Commit chunk data.
 *
 * Changing this requires a REPLICACHE_FORMAT_VERSION bump.
 */
export type ChunkIndexDefinition = {
  readonly name: string;
  readonly keyPrefix: string;
  readonly jsonPointer: string;
  // Used to not exist
  readonly allowEmpty?: boolean;
};

export function chunkIndexDefinitionEqualIgnoreName(
  a: ChunkIndexDefinition,
  b: ChunkIndexDefinition,
): boolean {
  return (
    a.jsonPointer === b.jsonPointer &&
    (a.allowEmpty ?? false) === (b.allowEmpty ?? false) &&
    a.keyPrefix === b.keyPrefix
  );
}

function assertChunkIndexDefinition(
  v: unknown,
): asserts v is ChunkIndexDefinition {
  assertObject(v);
  assertDeepFrozen(v);
  assertString(v.name);
  assertString(v.keyPrefix);
  assertString(v.jsonPointer);
  if (v.allowEmpty !== undefined) {
    assertBoolean(v.allowEmpty);
  }
}

export function toChunkIndexDefinition(
  name: string,
  indexDefinition: IndexDefinition,
): Required<ChunkIndexDefinition> {
  return {
    name,
    keyPrefix: indexDefinition.prefix ?? '',
    jsonPointer: indexDefinition.jsonPointer,
    allowEmpty: indexDefinition.allowEmpty ?? false,
  };
}

export type IndexRecord = {
  readonly definition: ChunkIndexDefinition;
  readonly valueHash: Hash;
};

function assertIndexRecord(v: unknown): asserts v is IndexRecord {
  assertObject(v);
  assertDeepFrozen(v);
  assertChunkIndexDefinition(v.definition);
  assertString(v.valueHash);
}

function assertIndexRecords(v: unknown): asserts v is IndexRecord[] {
  assertArray(v);
  assertDeepFrozen(v);
  for (const ir of v) {
    assertIndexRecord(ir);
  }
}

export function newLocalSDD(
  createChunk: dag.CreateChunk,
  basisHash: Hash,
  mutationID: number,
  mutatorName: string,
  mutatorArgsJSON: FrozenJSONValue,
  originalHash: Hash | null,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
  timestamp: number,
): Commit<LocalMetaSDD | LocalMetaDD31> {
  const meta: LocalMetaSDD = {
    type: MetaType.LocalSDD,
    basisHash,
    mutationID,
    mutatorName,
    mutatorArgsJSON,
    originalHash,
    timestamp,
  };
  return commitFromCommitData(
    createChunk,
    makeCommitData(meta, valueHash, indexes),
  );
}

export function newLocalDD31(
  createChunk: dag.CreateChunk,
  basisHash: Hash,
  baseSnapshotHash: Hash,
  mutationID: number,
  mutatorName: string,
  mutatorArgsJSON: FrozenJSONValue,
  originalHash: Hash | null,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
  timestamp: number,
  clientID: ClientID,
): Commit<LocalMetaDD31> {
  const meta: LocalMetaDD31 = {
    type: MetaType.LocalDD31,
    basisHash,
    baseSnapshotHash,
    mutationID,
    mutatorName,
    mutatorArgsJSON,
    originalHash,
    timestamp,
    clientID,
  };
  return commitFromCommitData(
    createChunk,
    makeCommitData(meta, valueHash, indexes),
  );
}

export function newSnapshotSDD(
  createChunk: dag.CreateChunk,
  basisHash: Hash | null,
  lastMutationID: number,
  cookieJSON: FrozenJSONValue,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
): Commit<SnapshotMetaSDD> {
  return commitFromCommitData(
    createChunk,
    newSnapshotCommitDataSDD(
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
  cookieJSON: FrozenCookie,
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

export function newSnapshotCommitDataSDD(
  basisHash: Hash | null,
  lastMutationID: number,
  cookieJSON: FrozenJSONValue,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
): CommitData<SnapshotMetaSDD> {
  const meta: SnapshotMetaSDD = {
    type: MetaType.SnapshotSDD,
    basisHash,
    lastMutationID,
    cookieJSON,
  };
  return makeCommitData(meta, valueHash, indexes);
}

export function newSnapshotCommitDataDD31(
  basisHash: Hash | null,
  lastMutationIDs: Record<ClientID, number>,
  cookieJSON: FrozenCookie,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
): CommitData<SnapshotMetaDD31> {
  const meta: SnapshotMetaDD31 = {
    type: MetaType.SnapshotDD31,
    basisHash,
    lastMutationIDs,
    cookieJSON,
  };
  return makeCommitData(meta, valueHash, indexes);
}

export function newIndexChange(
  createChunk: dag.CreateChunk,
  basisHash: Hash,
  lastMutationID: number,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
): Commit<IndexChangeMetaSDD> {
  const meta: IndexChangeMetaSDD = {
    type: MetaType.IndexChangeSDD,
    basisHash,
    lastMutationID,
  };
  return commitFromCommitData(
    createChunk,
    makeCommitData(meta, valueHash, indexes),
  );
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
    case MetaType.IndexChangeSDD:
      meta.basisHash && refs.push(meta.basisHash);
      break;
    case MetaType.LocalSDD:
    case MetaType.LocalDD31:
      meta.basisHash && refs.push(meta.basisHash);
      // Local has weak originalHash
      break;
    case MetaType.SnapshotSDD:
    case MetaType.SnapshotDD31:
      // Snapshot has weak basisHash
      break;
    default:
      unreachable();
  }

  for (const index of data.indexes) {
    refs.push(index.valueHash);
  }

  return refs;
}

export type CommitData<M extends Meta> = FrozenTag<{
  readonly meta: M;
  readonly valueHash: Hash;
  readonly indexes: readonly IndexRecord[];
}>;

export function makeCommitData<M extends Meta>(
  meta: M,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
): CommitData<M> {
  return deepFreeze({
    meta,
    valueHash,
    indexes,
  }) as unknown as CommitData<M>;
}

export function assertCommitData(v: unknown): asserts v is CommitData<Meta> {
  if (skipCommitDataAsserts) {
    return;
  }

  assertObject(v);
  assertDeepFrozen(v);
  assertMeta(v.meta);
  assertString(v.valueHash);
  assertIndexRecords(v.indexes);
}

function validateChunk(
  chunk: dag.Chunk,
): asserts chunk is dag.Chunk<CommitData<Meta>> {
  const {data} = chunk;
  assertCommitData(data);

  const seen = new Set();
  for (const index of data.indexes) {
    const {name} = index.definition;
    if (seen.has(name)) {
      throw new Error(`Duplicate index ${name}`);
    }
    seen.add(name);
  }
}
