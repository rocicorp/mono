import {assertHash, Hash} from '../hash';
import type * as sync from '../sync/mod';
import type * as dag from '../dag/mod';
import type {ReadonlyJSONValue} from '../json';
import {
  assert,
  assertArray,
  assertNumber,
  assertObject,
  assertString,
} from '../asserts';
import {
  assertIndexDefinitions,
  IndexDefinitions,
  indexDefinitionsEqual,
} from '../index-defs';

export type BranchMap = ReadonlyMap<sync.BranchID, Branch>;

export type Branch = {
  /**
   * The hash of the commit in the perdag last persisted to this branch.
   * Should only be updated by clients assigned to this branch.
   */
  readonly headHash: Hash;

  /**
   * Set of mutator names common to all clients assigned to this branch.
   */
  readonly mutatorNames: string[];

  /**
   * Index definitions common to all clients assigned to this branch.
   */
  readonly indexes: IndexDefinitions;

  /**
   * The highest mutation ID of every client assigned to this branch.
   * Should only be updated by clients assigned to this branch.
   * Read by other clients to determine if there are unacknowledged pending
   * mutations for them to try to recover.
   * This is redundant with information in the commit graph at `headHash`,
   * but allows other clients to determine if there are unacknowledged pending
   * mutations without having to load the commit graph.
   */
  readonly mutationIDs: Record<sync.ClientID, number>;

  /**
   * The highest lastMutationID received from the server for every client
   * assigned to this branch.
   *
   * Should be updated by the clients assigned to this branch whenever they
   * persist to this branch.
   * Read by other clients to determine if there are unacknowledged pending
   * mutations for them to recover and *updated* by other clients upon
   * successfully recovering pending mutations to avoid redundant pushes of
   * pending mutations.
   *
   * Note: This will be the same as the `lastMutationIDs` of the
   * base snapshot of the branch's commit graph when written
   * by clients assigned to this branch.  However, when written by another
   * client recovering mutations it may be different because the other client
   * does not update the commit graph.
   */
  readonly lastServerAckdMutationIDs: Record<sync.ClientID, number>;
};

export const BRANCHES_HEAD_NAME = 'branches';

function assertBranch(value: unknown): asserts value is Branch {
  assertObject(value);
  const {
    headHash,
    mutatorNames,
    indexes,
    mutationIDs,
    lastServerAckdMutationIDs,
  } = value;
  assertHash(headHash);
  assertArray(mutatorNames);
  for (const name of mutatorNames) {
    assertString(name);
  }
  assertObject(indexes);
  assertIndexDefinitions(indexes);
  assertObject(mutationIDs);
  for (const mutationID of Object.values(mutationIDs)) {
    assertNumber(mutationID);
  }
  assertObject(lastServerAckdMutationIDs);
  for (const mutationID of Object.values(lastServerAckdMutationIDs)) {
    assertNumber(mutationID);
  }
}

function chunkDataToBranchMap(chunkData: unknown): BranchMap {
  assertObject(chunkData);
  const branches = new Map<sync.BranchID, Branch>();
  for (const [key, value] of Object.entries(chunkData)) {
    if (value !== undefined) {
      assertBranch(value);
      branches.set(key, value);
    }
  }
  return branches;
}

function branchMapToChunkData(
  branches: BranchMap,
  dagWrite: dag.Write,
): ReadonlyJSONValue {
  const chunkData: {[id: sync.BranchID]: Branch} = {};
  for (const [branchID, branch] of branches.entries()) {
    dagWrite.assertValidHash(branch.headHash);
    chunkData[branchID] = {
      ...branch,
      mutatorNames: [...branch.mutatorNames.values()],
    };
  }
  return chunkData;
}

async function getBranchesAtHash(
  hash: Hash,
  dagRead: dag.Read,
): Promise<BranchMap> {
  const chunk = await dagRead.getChunk(hash);
  return chunkDataToBranchMap(chunk?.data);
}

export async function getBranches(dagRead: dag.Read): Promise<BranchMap> {
  const hash = await dagRead.getHead(BRANCHES_HEAD_NAME);
  if (!hash) {
    return new Map();
  }
  return getBranchesAtHash(hash, dagRead);
}

export async function setBranches(
  branches: BranchMap,
  dagWrite: dag.Write,
): Promise<BranchMap> {
  const currBranches = await getBranches(dagWrite);
  for (const [branchID, branch] of branches) {
    const currBranch = currBranches.get(branchID);
    validateBranchUpdate(branch, currBranch);
  }
  return setValidatedBranches(branches, dagWrite);
}

export async function setBranch(
  branchID: sync.BranchID,
  branch: Branch,
  dagWrite: dag.Write,
): Promise<BranchMap> {
  const currBranches = await getBranches(dagWrite);
  const currBranch = currBranches.get(branchID);
  validateBranchUpdate(branch, currBranch);
  const newBranches = new Map(currBranches);
  newBranches.set(branchID, branch);
  return setValidatedBranches(newBranches, dagWrite);
}

export async function deleteBranch(
  branchID: sync.BranchID,
  dagWrite: dag.Write,
): Promise<BranchMap> {
  const currBranches = await getBranches(dagWrite);
  if (!currBranches.has(branchID)) {
    return currBranches;
  }
  const newBranches = new Map(currBranches.entries());
  newBranches.delete(branchID);
  return setValidatedBranches(newBranches, dagWrite);
}

function validateBranchUpdate(branch: Branch, currBranch: Branch | undefined) {
  const mutatorNamesSet = new Set(branch.mutatorNames);
  assert(
    mutatorNamesSet.size === branch.mutatorNames.length,
    "A branch's mutatorNames must be a set.",
  );
  if (currBranch !== undefined) {
    assert(
      indexDefinitionsEqual(currBranch.indexes, branch.indexes),
      "A branch's index definitions must never change.",
    );
    assert(
      mutatorNamesEqual(mutatorNamesSet, currBranch.mutatorNames),
      "A branch's mutatorNames must never change.",
    );
  }
}

async function setValidatedBranches(
  branches: BranchMap,
  dagWrite: dag.Write,
): Promise<BranchMap> {
  const chunkData = branchMapToChunkData(branches, dagWrite);
  const refs = Array.from(branches.values(), branch => branch.headHash);
  const chunk = dagWrite.createChunk(chunkData, refs);
  await dagWrite.putChunk(chunk);
  await dagWrite.setHead(BRANCHES_HEAD_NAME, chunk.hash);
  return branches;
}

export function mutatorNamesEqual(
  mutatorNamesSet: ReadonlySet<string>,
  mutatorNames: string[],
): boolean {
  if (mutatorNames.length !== mutatorNamesSet.size) {
    return false;
  }
  for (const mutatorName of mutatorNames) {
    if (!mutatorNamesSet.has(mutatorName)) {
      return false;
    }
  }
  return true;
}

export async function getBranch(
  id: sync.BranchID,
  dagRead: dag.Read,
): Promise<Branch | undefined> {
  const branches = await getBranches(dagRead);
  return branches.get(id);
}

/**
 * Used to signal that a branch does not exist. Maybe it was garbage collected?
 */
export class BranchStateNotFoundError extends Error {
  name = 'BranchStateNotFoundError';
  readonly id: string;
  constructor(id: sync.BranchID) {
    super(`Branch state not found, id: ${id}`);
    this.id = id;
  }
}

/**
 * Throws a `BranchStateNotFoundError` if the branch does not exist.
 */
export async function assertHasBranchState(
  id: sync.BranchID,
  dagRead: dag.Read,
): Promise<void> {
  if (!(await hasBranchState(id, dagRead))) {
    throw new BranchStateNotFoundError(id);
  }
}

export async function hasBranchState(
  id: sync.BranchID,
  dagRead: dag.Read,
): Promise<boolean> {
  return !!(await getBranch(id, dagRead));
}

export function branchHasPendingMutations(branch: Branch) {
  for (const [clientID, mutationID] of Object.entries(branch.mutationIDs)) {
    const lastServerAckdMutationID = branch.lastServerAckdMutationIDs[clientID];
    if (
      (lastServerAckdMutationID === undefined && mutationID !== 0) ||
      lastServerAckdMutationID < mutationID
    ) {
      return true;
    }
  }
  return false;
}
