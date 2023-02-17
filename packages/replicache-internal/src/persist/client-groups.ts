import {assertHash, Hash} from '../hash.js';
import type * as sync from '../sync/mod.js';
import type * as dag from '../dag/mod.js';
import {FrozenJSONValue, deepFreeze} from '../json.js';
import {
  assert,
  assertArray,
  assertNumber,
  assertObject,
  assertString,
} from '../asserts.js';
import {
  assertIndexDefinitions,
  IndexDefinitions,
  indexDefinitionsEqual,
} from '../index-defs.js';

export type ClientGroupMap = ReadonlyMap<sync.ClientGroupID, ClientGroup>;

export type ClientGroup = {
  /**
   * The hash of the commit in the perdag last persisted to this client group.
   * Should only be updated by clients assigned to this client group.
   */
  readonly headHash: Hash;

  /**
   * Set of mutator names common to all clients assigned to this client group.
   */
  readonly mutatorNames: string[];

  /**
   * Index definitions common to all clients assigned to this client group.
   */
  readonly indexes: IndexDefinitions;

  /**
   * The highest mutation ID of every client assigned to this client group.
   * Should only be updated by clients assigned to this client group. Read by
   * other clients to determine if there are unacknowledged pending mutations
   * for them to try to recover. This is redundant with information in the
   * commit graph at `headHash`, but allows other clients to determine if there
   * are unacknowledged pending mutations without having to load the commit
   * graph.
   */
  readonly mutationIDs: Record<sync.ClientID, number>;

  /**
   * The highest lastMutationID received from the server for every client
   * assigned to this client group.
   *
   * Should be updated by the clients assigned to this client group whenever
   * they persist to this client group. Read by other clients to determine if
   * there are unacknowledged pending mutations for them to recover and
   * *updated* by other clients upon successfully recovering pending mutations
   * to avoid redundant pushes of pending mutations.
   *
   * Note: This will be the same as the `lastMutationIDs` of the base snapshot
   * of the client group's commit graph when written by clients assigned to this
   * client group.  However, when written by another client recovering mutations
   * it may be different because the other client does not update the commit
   * graph.
   */
  readonly lastServerAckdMutationIDs: Record<sync.ClientID, number>;

  /**
   * If the server deletes this client group it can signal that the client group
   * was deleted. If that happens we mark this client group as disabled so that
   * we do not use it again when creating new clients.
   */
  readonly disabled: boolean;
};

export const CLIENT_GROUPS_HEAD_NAME = 'client-groups';

function assertClientGroup(value: unknown): asserts value is ClientGroup {
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

function chunkDataToClientGroupMap(chunkData: unknown): ClientGroupMap {
  assertObject(chunkData);
  const clientGroups = new Map<sync.ClientGroupID, ClientGroup>();
  for (const [key, value] of Object.entries(chunkData)) {
    if (value !== undefined) {
      assertClientGroup(value);
      clientGroups.set(key, value);
    }
  }
  return clientGroups;
}

function clientGroupMapToChunkData(
  clientGroups: ClientGroupMap,
  dagWrite: dag.Write,
): FrozenJSONValue {
  const chunkData: {[id: sync.ClientGroupID]: ClientGroup} = {};
  for (const [clientGroupID, clientGroup] of clientGroups.entries()) {
    dagWrite.assertValidHash(clientGroup.headHash);
    chunkData[clientGroupID] = {
      ...clientGroup,
      mutatorNames: [...clientGroup.mutatorNames.values()],
    };
  }
  return deepFreeze(chunkData);
}

async function getClientGroupsAtHash(
  hash: Hash,
  dagRead: dag.Read,
): Promise<ClientGroupMap> {
  const chunk = await dagRead.getChunk(hash);
  return chunkDataToClientGroupMap(chunk?.data);
}

export async function getClientGroups(
  dagRead: dag.Read,
): Promise<ClientGroupMap> {
  const hash = await dagRead.getHead(CLIENT_GROUPS_HEAD_NAME);
  if (!hash) {
    return new Map();
  }
  return getClientGroupsAtHash(hash, dagRead);
}

export async function setClientGroups(
  clientGroups: ClientGroupMap,
  dagWrite: dag.Write,
): Promise<ClientGroupMap> {
  const currClientGroups = await getClientGroups(dagWrite);
  for (const [clientGroupID, clientGroup] of clientGroups) {
    const currClientGroup = currClientGroups.get(clientGroupID);
    validateClientGroupUpdate(clientGroup, currClientGroup);
  }
  return setValidatedClientGroups(clientGroups, dagWrite);
}

export async function setClientGroup(
  clientGroupID: sync.ClientGroupID,
  clientGroup: ClientGroup,
  dagWrite: dag.Write,
): Promise<ClientGroupMap> {
  const currClientGroups = await getClientGroups(dagWrite);
  const currClientGroup = currClientGroups.get(clientGroupID);
  validateClientGroupUpdate(clientGroup, currClientGroup);
  const newClientGroups = new Map(currClientGroups);
  newClientGroups.set(clientGroupID, clientGroup);
  return setValidatedClientGroups(newClientGroups, dagWrite);
}

export async function deleteClientGroup(
  clientGroupID: sync.ClientGroupID,
  dagWrite: dag.Write,
): Promise<ClientGroupMap> {
  const currClientGroups = await getClientGroups(dagWrite);
  if (!currClientGroups.has(clientGroupID)) {
    return currClientGroups;
  }
  const newClientGroups = new Map(currClientGroups.entries());
  newClientGroups.delete(clientGroupID);
  return setValidatedClientGroups(newClientGroups, dagWrite);
}

function validateClientGroupUpdate(
  clientGroup: ClientGroup,
  currClientGroup: ClientGroup | undefined,
) {
  const mutatorNamesSet = new Set(clientGroup.mutatorNames);
  assert(
    mutatorNamesSet.size === clientGroup.mutatorNames.length,
    "A client group's mutatorNames must be a set.",
  );
  if (currClientGroup !== undefined) {
    assert(
      indexDefinitionsEqual(currClientGroup.indexes, clientGroup.indexes),
      "A client group's index definitions must never change.",
    );
    assert(
      mutatorNamesEqual(mutatorNamesSet, currClientGroup.mutatorNames),
      "A client group's mutatorNames must never change.",
    );
  }
}

async function setValidatedClientGroups(
  clientGroups: ClientGroupMap,
  dagWrite: dag.Write,
): Promise<ClientGroupMap> {
  const chunkData = clientGroupMapToChunkData(clientGroups, dagWrite);
  const refs = Array.from(
    clientGroups.values(),
    clientGroup => clientGroup.headHash,
  );
  const chunk = dagWrite.createChunk(chunkData, refs);
  await dagWrite.putChunk(chunk);
  await dagWrite.setHead(CLIENT_GROUPS_HEAD_NAME, chunk.hash);
  return clientGroups;
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

export async function getClientGroup(
  id: sync.ClientGroupID,
  dagRead: dag.Read,
): Promise<ClientGroup | undefined> {
  const clientGroups = await getClientGroups(dagRead);
  return clientGroups.get(id);
}

export function clientGroupHasPendingMutations(clientGroup: ClientGroup) {
  for (const [clientID, mutationID] of Object.entries(
    clientGroup.mutationIDs,
  )) {
    const lastServerAckdMutationID =
      clientGroup.lastServerAckdMutationIDs[clientID];
    if (
      (lastServerAckdMutationID === undefined && mutationID !== 0) ||
      lastServerAckdMutationID < mutationID
    ) {
      return true;
    }
  }
  return false;
}

/**
 * Marks a client group as disabled. This can happen if the server deletes the
 * client group (servers should not delete clients or client groups but it often
 * happens in practice when developing).
 *
 * A disabled client group prevents pulls and pushes from happening.
 */
export async function disableClientGroup(
  clientGroupID: string,
  dagWrite: dag.Write,
): Promise<void> {
  const clientGroup = await getClientGroup(clientGroupID, dagWrite);
  if (!clientGroup) {
    // No client group matching in the database, so nothing to do.
    return;
  }
  const disabledClientGroup = {
    ...clientGroup,
    disabled: true,
  };
  await setClientGroup(clientGroupID, disabledClientGroup, dagWrite);
}
