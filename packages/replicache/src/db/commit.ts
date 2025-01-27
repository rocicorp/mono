import {
  assert,
  assertArray,
  assertBoolean,
  assertNumber,
  assertObject,
  assertString,
  unreachable,
} from '../../../shared/src/asserts.ts';
import {assertJSONValue} from '../../../shared/src/json.ts';
import {skipCommitDataAsserts} from '../config.ts';
import {type FrozenCookie, compareCookies} from '../cookies.ts';
import {type Chunk, type CreateChunk, type Refs, toRefs} from '../dag/chunk.ts';
import {type MustGetChunk, type Read, mustGetHeadHash} from '../dag/store.ts';
import {
  type FrozenJSONValue,
  type FrozenTag,
  assertDeepFrozen,
  deepFreeze,
} from '../frozen-json.ts';
import {type Hash, assertHash} from '../hash.ts';
import type {IndexDefinition} from '../index-defs.ts';
import type {ClientID} from '../sync/ids.ts';
import * as MetaType from './meta-type-enum.ts';

export const DEFAULT_HEAD_NAME = 'main';

export function commitIsLocalDD31(
  commit: Commit<Meta>,
): commit is Commit<LocalMetaDD31> {
  return isLocalMetaDD31(commit.meta);
}

export function commitIsLocal(
  commit: Commit<Meta>,
): commit is Commit<LocalMetaDD31> {
  return commitIsLocalDD31(commit);
}

export function commitIsSnapshot(
  commit: Commit<Meta>,
): commit is Commit<SnapshotMetaDD31> {
  return isSnapshotMetaDD31(commit.meta);
}

export class Commit<M extends Meta> {
  readonly chunk: Chunk<CommitData<M>>;

  constructor(chunk: Chunk<CommitData<M>>) {
    this.chunk = chunk;
  }

  get meta(): M {
    return this.chunk.data.meta;
  }

  get valueHash(): Hash {
    // Already validated!
    return this.chunk.data.valueHash;
  }

  getMutationID(clientID: ClientID, dagRead: MustGetChunk): Promise<number> {
    return getMutationID(clientID, dagRead, this.meta);
  }

  async getNextMutationID(
    clientID: ClientID,
    dagRead: MustGetChunk,
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
  dagRead: MustGetChunk,
  meta: Meta,
): Promise<number> {
  switch (meta.type) {
    case MetaType.SnapshotDD31:
      return meta.lastMutationIDs[clientID] ?? 0;

    case MetaType.LocalDD31: {
      if (meta.clientID === clientID) {
        return meta.mutationID;
      }
      const {basisHash} = meta;
      const basisCommit = await commitFromHash(basisHash, dagRead);
      return getMutationID(clientID, dagRead, basisCommit.meta);
    }

    default:
      unreachable(meta);
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
  dagRead: Read,
): Promise<Commit<LocalMetaDD31>[]> {
  const commits = await commitChain(fromCommitHash, dagRead);
  // Filter does not deal with type narrowing.
  return commits.filter(c => commitIsLocal(c)) as Commit<LocalMetaDD31>[];
}

export async function localMutationsDD31(
  fromCommitHash: Hash,
  dagRead: Read,
): Promise<Commit<LocalMetaDD31>[]> {
  const commits = await commitChain(fromCommitHash, dagRead);
  // Filter does not deal with type narrowing.
  return commits.filter(c => commitIsLocalDD31(c)) as Commit<LocalMetaDD31>[];
}

export async function localMutationsGreaterThan(
  commit: Commit<Meta>,
  mutationIDLimits: Record<ClientID, number>,
  dagRead: Read,
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
  dagRead: Read,
): Promise<Commit<SnapshotMetaDD31>> {
  const hash = await dagRead.getHead(name);
  assert(hash, `Missing head ${name}`);
  return baseSnapshotFromHash(hash, dagRead);
}

export async function baseSnapshotHashFromHash(
  hash: Hash,
  dagRead: Read,
): Promise<Hash> {
  return (await baseSnapshotFromHash(hash, dagRead)).chunk.hash;
}

export async function baseSnapshotFromHash(
  hash: Hash,
  dagRead: Read,
): Promise<Commit<SnapshotMetaDD31>> {
  const commit = await commitFromHash(hash, dagRead);
  return baseSnapshotFromCommit(commit, dagRead);
}

export async function baseSnapshotFromCommit(
  commit: Commit<Meta>,
  dagRead: Read,
): Promise<Commit<SnapshotMetaDD31>> {
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
  c: Commit<SnapshotMetaDD31>,
  clientID: ClientID,
): [lastMutationID: number, cookie: FrozenCookie | FrozenJSONValue] {
  const m = c.meta;
  const lmid = m.lastMutationIDs[clientID] ?? 0;
  return [lmid, m.cookieJSON];
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
  dagRead: Read,
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
  dagRead: Read,
): Promise<Commit<Meta>> {
  const hash = await mustGetHeadHash(name, dagRead);
  return commitFromHash(hash, dagRead);
}

export type LocalMetaDD31 = {
  readonly type: MetaType.LocalDD31;
  readonly basisHash: Hash;
  readonly mutationID: number;
  readonly mutatorName: string;
  readonly mutatorArgsJSON: FrozenJSONValue;
  readonly originalHash: Hash | null;
  readonly timestamp: number;
  readonly clientID: ClientID;
  readonly baseSnapshotHash: Hash;
};

export type LocalMeta = LocalMetaDD31;

export function assertLocalMetaDD31(
  v: Record<string, unknown>,
): asserts v is LocalMetaDD31 {
  // type already asserted
  assertString(v.clientID);
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

export function isLocalMetaDD31(meta: Meta): meta is LocalMetaDD31 {
  return meta.type === MetaType.LocalDD31;
}

export function assertLocalCommitDD31(
  c: Commit<Meta>,
): asserts c is Commit<LocalMetaDD31> {
  assertLocalMetaDD31(c.meta);
}

export type SnapshotMetaDD31 = {
  readonly type: MetaType.SnapshotDD31;
  readonly basisHash: Hash | null;
  readonly lastMutationIDs: Record<ClientID, number>;
  readonly cookieJSON: FrozenCookie;
};

export type SnapshotMeta = SnapshotMetaDD31;

export function assertSnapshotMetaDD31(
  v: Record<string, unknown>,
): asserts v is SnapshotMetaDD31 {
  // type already asserted
  if (v.basisHash !== null) {
    assertHash(v.basisHash);
  }
  assertJSONValue(v.cookieJSON);
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

export type Meta = LocalMetaDD31 | SnapshotMetaDD31;

export function assertSnapshotCommitDD31(
  c: Commit<Meta>,
): asserts c is Commit<SnapshotMetaDD31> {
  assertSnapshotMetaDD31(c.meta);
}

function isSnapshotMetaDD31(meta: Meta): meta is SnapshotMetaDD31 {
  return meta.type === MetaType.SnapshotDD31;
}

function assertMeta(v: unknown): asserts v is Meta {
  assertObject(v);
  assertDeepFrozen(v);
  if (v.basisHash !== null) {
    assertString(v.basisHash);
  }

  assertNumber(v.type);
  switch (v.type) {
    case MetaType.LocalDD31:
      assertLocalMetaDD31(v);
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

export function newLocalDD31(
  createChunk: CreateChunk,
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

export function newSnapshotDD31(
  createChunk: CreateChunk,
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

export function fromChunk(chunk: Chunk): Commit<Meta> {
  validateChunk(chunk);
  return new Commit(chunk);
}

function commitFromCommitData<M extends Meta>(
  createChunk: CreateChunk,
  data: CommitData<M>,
): Commit<M> {
  return new Commit(createChunk(data, getRefs(data)));
}

export function getRefs(data: CommitData<Meta>): Refs {
  const refs: Set<Hash> = new Set();
  refs.add(data.valueHash);
  const {meta} = data;
  switch (meta.type) {
    case MetaType.LocalDD31:
      meta.basisHash && refs.add(meta.basisHash);
      // Local has weak originalHash
      break;
    case MetaType.SnapshotDD31:
      // Snapshot has weak basisHash
      break;
    default:
      unreachable(meta);
  }

  for (const index of data.indexes) {
    refs.add(index.valueHash);
  }

  return toRefs(refs);
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

function validateChunk(chunk: Chunk): asserts chunk is Chunk<CommitData<Meta>> {
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
