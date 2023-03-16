import type {LogContext} from '@rocicorp/logger';
import {assertHash, Hash} from '../hash.js';
import * as btree from '../btree/mod.js';
import type * as dag from '../dag/mod.js';
import * as db from '../db/mod.js';
import type * as sync from '../sync/mod.js';
import {FrozenJSONValue, deepFreeze} from '../json.js';
import {
  assert,
  assertNumber,
  assertObject,
  assertString,
} from 'shared/asserts.js';
import {hasOwn} from 'shared/has-own.js';
import {uuid as makeUuid} from '../uuid.js';
import {
  assertSnapshotCommitDD31,
  getRefs,
  toChunkIndexDefinition,
  newSnapshotCommitDataDD31,
  ChunkIndexDefinition,
  chunkIndexDefinitionEqualIgnoreName,
} from '../db/commit.js';
import {compareCookies, FrozenCookie} from '../cookies.js';
import type {ClientID} from '../sync/ids.js';
import {
  ClientGroup,
  getClientGroup,
  getClientGroups,
  mutatorNamesEqual,
  setClientGroup,
} from './client-groups.js';
import {IndexDefinitions, indexDefinitionsEqual} from '../index-defs.js';
import {createIndexBTree} from '../db/write.js';
import {withWrite} from '../with-transactions.js';

export type ClientMap = ReadonlyMap<sync.ClientID, ClientV4 | ClientV5>;
export type ClientMapDD31 = ReadonlyMap<sync.ClientID, ClientV5>;

export type ClientV4 = {
  /**
   * A UNIX timestamp in milliseconds updated by the client once a minute
   * while it is active and every time the client persists its state to
   * the perdag.
   * Should only be updated by the client represented by this structure.
   */
  readonly heartbeatTimestampMs: number;
  /**
   * The hash of the commit in the perdag this client last persisted.
   * Should only be updated by the client represented by this structure.
   */
  readonly headHash: Hash;
  /**
   * The mutationID of the commit at headHash (mutationID if it is a
   * local commit, lastMutationID if it is an index change or snapshot commit).
   * Should only be updated by the client represented by this structure.
   * Read by other clients to determine if there are unacknowledged pending
   * mutations for them to push on behalf of the client represented by this
   * structure.
   * This is redundant with information in the commit graph at headHash,
   * but allows other clients to determine if there are unacknowledged pending
   * mutations without having to load the commit graph at headHash.
   */
  readonly mutationID: number;
  /**
   * The highest lastMutationID received from the server for this client.
   *
   * Should be updated by the client represented by this structure whenever
   * it persists its state to the perdag.
   * Read by other clients to determine if there are unacknowledged pending
   * mutations for them to push on behalf of the client represented by this
   * structure, and *updated* by other clients upon successfully pushing
   * pending mutations to avoid redundant pushes of those mutations.
   *
   * Note: This will be the same as the lastMutationID of the base snapshot of
   * the commit graph at headHash when written by the client represented by this
   * structure.  However, when written by another client pushing pending
   * mutations on this client's behalf it will be different.  This is because
   * the other client does not update the commit graph (it is unsafe to update
   * another client's commit graph).
   */
  readonly lastServerAckdMutationID: number;
};

export type ClientV5 = {
  readonly heartbeatTimestampMs: number;
  readonly headHash: Hash;

  /**
   * The hash of a commit we are in the middle of refreshing into this client's
   * memdag.
   */
  readonly tempRefreshHash: Hash | null;

  /**
   * ID of this client's perdag client group. This needs to be sent in pull
   * request (to enable syncing all last mutation ids in the client group).
   */
  readonly clientGroupID: sync.ClientGroupID;
};

export type Client = ClientV4 | ClientV5;

function isClientV5(client: Client): client is ClientV5 {
  return (client as ClientV5).clientGroupID !== undefined;
}

export function isClientV4(client: Client): client is ClientV4 {
  return (client as ClientV4).lastServerAckdMutationID !== undefined;
}

export const CLIENTS_HEAD_NAME = 'clients';

function assertClient(value: unknown): asserts value is Client {
  assertClientBase(value);

  if (typeof value.mutationID === 'number') {
    assertNumber(value.lastServerAckdMutationID);
  } else {
    const {tempRefreshHash} = value;
    if (tempRefreshHash) {
      assertHash(tempRefreshHash);
    }
    assertString(value.clientGroupID);
  }
}

function assertClientBase(value: unknown): asserts value is {
  heartbeatTimestampMs: number;
  headHash: Hash;
  [key: string]: unknown;
} {
  assertObject(value);
  const {heartbeatTimestampMs, headHash} = value;
  assertNumber(heartbeatTimestampMs);
  assertHash(headHash);
}

export function assertClientV4(value: unknown): asserts value is ClientV4 {
  assertClientBase(value);
  const {mutationID, lastServerAckdMutationID} = value;
  assertNumber(mutationID);
  assertNumber(lastServerAckdMutationID);
}

export function assertClientV5(value: unknown): asserts value is ClientV5 {
  assertClientBase(value);
  const {tempRefreshHash} = value;
  if (tempRefreshHash) {
    assertHash(tempRefreshHash);
  }
  assertString(value.clientGroupID);
}

function chunkDataToClientMap(chunkData: unknown): ClientMap {
  assertObject(chunkData);
  const clients = new Map();
  for (const key in chunkData) {
    if (hasOwn(chunkData, key)) {
      const value = chunkData[key];
      if (value !== undefined) {
        assertClient(value);
        clients.set(key, value);
      }
    }
  }
  return clients;
}

function clientMapToChunkData(
  clients: ClientMap,
  dagWrite: dag.Write,
): FrozenJSONValue {
  for (const client of clients.values()) {
    dagWrite.assertValidHash(client.headHash);
    if (isClientV5(client) && client.tempRefreshHash) {
      dagWrite.assertValidHash(client.tempRefreshHash);
    }
  }
  return deepFreeze(Object.fromEntries(clients));
}

export async function getClients(dagRead: dag.Read): Promise<ClientMap> {
  const hash = await dagRead.getHead(CLIENTS_HEAD_NAME);
  return getClientsAtHash(hash, dagRead);
}

async function getClientsAtHash(
  hash: Hash | undefined,
  dagRead: dag.Read,
): Promise<ClientMap> {
  if (!hash) {
    return new Map();
  }
  const chunk = await dagRead.getChunk(hash);
  return chunkDataToClientMap(chunk?.data);
}

/**
 * Used to signal that a client does not exist. Maybe it was garbage collected?
 */
export class ClientStateNotFoundError extends Error {
  name = 'ClientStateNotFoundError';
  readonly id: string;
  constructor(id: sync.ClientID) {
    super(`Client state not found, id: ${id}`);
    this.id = id;
  }
}

/**
 * Throws a `ClientStateNotFoundError` if the client does not exist.
 */
export async function assertHasClientState(
  id: sync.ClientID,
  dagRead: dag.Read,
): Promise<void> {
  if (!(await hasClientState(id, dagRead))) {
    throw new ClientStateNotFoundError(id);
  }
}

export async function hasClientState(
  id: sync.ClientID,
  dagRead: dag.Read,
): Promise<boolean> {
  return !!(await getClient(id, dagRead));
}

export async function getClient(
  id: sync.ClientID,
  dagRead: dag.Read,
): Promise<Client | undefined> {
  const clients = await getClients(dagRead);
  return clients.get(id);
}

export function initClientV5(
  lc: LogContext,
  perdag: dag.Store,
  mutatorNames: string[],
  indexes: IndexDefinitions,
): Promise<
  [
    clientID: sync.ClientID,
    client: ClientV5,
    clientMap: ClientMap,
    newClientGroup: boolean,
  ]
> {
  return withWrite(perdag, async dagWrite => {
    async function setClientsAndClientGroupAndCommit(
      basisHash: Hash | null,
      cookieJSON: FrozenCookie,
      valueHash: Hash,
      indexRecords: readonly db.IndexRecord[],
    ): Promise<[sync.ClientID, ClientV5, ClientMap, boolean]> {
      const newSnapshotData = newSnapshotCommitDataDD31(
        basisHash,
        {},
        cookieJSON,
        valueHash,
        indexRecords,
      );
      const chunk = dagWrite.createChunk(
        newSnapshotData,
        getRefs(newSnapshotData),
      );

      const newClientGroupID = makeUuid();

      const newClient: ClientV5 = {
        heartbeatTimestampMs: Date.now(),
        headHash: chunk.hash,
        tempRefreshHash: null,
        clientGroupID: newClientGroupID,
      };

      const newClients = new Map(clients).set(newClientID, newClient);

      const clientGroup: ClientGroup = {
        headHash: chunk.hash,
        mutatorNames,
        indexes,
        mutationIDs: {},
        lastServerAckdMutationIDs: {},
        disabled: false,
      };

      await Promise.all([
        dagWrite.putChunk(chunk),
        setClients(newClients, dagWrite),
        setClientGroup(newClientGroupID, clientGroup, dagWrite),
      ]);

      await dagWrite.commit();

      return [newClientID, newClient, newClients, true];
    }

    const newClientID = makeUuid();
    const clients = await getClients(dagWrite);

    const res = await findMatchingClient(dagWrite, mutatorNames, indexes);
    if (res.type === FIND_MATCHING_CLIENT_TYPE_HEAD) {
      // We found a client group with matching mutators and indexes. We can
      // reuse it.
      const {clientGroupID, headHash} = res;

      const newClient: ClientV5 = {
        clientGroupID,
        headHash,
        heartbeatTimestampMs: Date.now(),
        tempRefreshHash: null,
      };
      const newClients = new Map(clients).set(newClientID, newClient);
      await setClients(newClients, dagWrite);

      await dagWrite.commit();
      return [newClientID, newClient, newClients, false];
    }

    if (res.type === FIND_MATCHING_CLIENT_TYPE_NEW) {
      // No client group to fork from. Create empty snapshot.
      const emptyBTreeChunk = dagWrite.createChunk(btree.emptyDataNode, []);
      await dagWrite.putChunk(emptyBTreeChunk);

      // Create indexes
      const indexRecords: db.IndexRecord[] = [];

      // At this point the value of replicache is the empty tree so all index
      // maps will also be the empty tree.
      for (const [name, indexDefinition] of Object.entries(indexes)) {
        const chunkIndexDefinition = toChunkIndexDefinition(
          name,
          indexDefinition,
        );
        indexRecords.push({
          definition: chunkIndexDefinition,
          valueHash: emptyBTreeChunk.hash,
        });
      }

      return setClientsAndClientGroupAndCommit(
        null,
        null,
        emptyBTreeChunk.hash,
        indexRecords,
      );
    }

    // Now we create a new client and client group that we fork from the found
    // snapshot.
    assert(res.type === FIND_MATCHING_CLIENT_TYPE_FORK);

    const {snapshot} = res;

    // Create indexes
    const indexRecords: db.IndexRecord[] = [];
    const {valueHash, indexes: oldIndexes} = snapshot;
    const map = new btree.BTreeRead(dagWrite, valueHash);

    for (const [name, indexDefinition] of Object.entries(indexes)) {
      const {prefix = '', jsonPointer, allowEmpty = false} = indexDefinition;
      const chunkIndexDefinition: ChunkIndexDefinition = {
        name,
        keyPrefix: prefix,
        jsonPointer,
        allowEmpty,
      };

      const oldIndex = findMatchingOldIndex(oldIndexes, chunkIndexDefinition);
      if (oldIndex) {
        indexRecords.push({
          definition: chunkIndexDefinition,
          valueHash: oldIndex.valueHash,
        });
      } else {
        const indexBTree = await createIndexBTree(
          lc,
          dagWrite,
          map,
          prefix,
          jsonPointer,
          allowEmpty,
        );
        indexRecords.push({
          definition: chunkIndexDefinition,
          valueHash: await indexBTree.flush(),
        });
      }
    }

    return setClientsAndClientGroupAndCommit(
      snapshot.meta.basisHash,
      snapshot.meta.cookieJSON,
      snapshot.valueHash,
      indexRecords,
    );
  });
}

function findMatchingOldIndex(
  oldIndexes: readonly db.IndexRecord[],
  chunkIndexDefinition: ChunkIndexDefinition,
) {
  return oldIndexes.find(index =>
    chunkIndexDefinitionEqualIgnoreName(index.definition, chunkIndexDefinition),
  );
}

export const FIND_MATCHING_CLIENT_TYPE_NEW = 0;
export const FIND_MATCHING_CLIENT_TYPE_FORK = 1;
export const FIND_MATCHING_CLIENT_TYPE_HEAD = 2;

export type FindMatchingClientResult =
  | {
      type: typeof FIND_MATCHING_CLIENT_TYPE_NEW;
    }
  | {
      type: typeof FIND_MATCHING_CLIENT_TYPE_FORK;
      snapshot: db.Commit<db.SnapshotMetaDD31>;
    }
  | {
      type: typeof FIND_MATCHING_CLIENT_TYPE_HEAD;
      clientGroupID: sync.ClientGroupID;
      headHash: Hash;
    };

export async function findMatchingClient(
  dagRead: dag.Read,
  mutatorNames: string[],
  indexes: IndexDefinitions,
): Promise<FindMatchingClientResult> {
  let newestCookie: FrozenCookie | undefined;
  let bestSnapshot: db.Commit<db.SnapshotMetaDD31> | undefined;
  const mutatorNamesSet = new Set(mutatorNames);

  const clientGroups = await getClientGroups(dagRead);
  for (const [clientGroupID, clientGroup] of clientGroups) {
    if (
      !clientGroup.disabled &&
      mutatorNamesEqual(mutatorNamesSet, clientGroup.mutatorNames) &&
      indexDefinitionsEqual(indexes, clientGroup.indexes)
    ) {
      // exact match
      return {
        type: FIND_MATCHING_CLIENT_TYPE_HEAD,
        clientGroupID,
        headHash: clientGroup.headHash,
      };
    }

    const clientGroupSnapshotCommit = await db.baseSnapshotFromHash(
      clientGroup.headHash,
      dagRead,
    );
    assertSnapshotCommitDD31(clientGroupSnapshotCommit);

    const {cookieJSON} = clientGroupSnapshotCommit.meta;
    if (
      newestCookie === undefined ||
      compareCookies(cookieJSON, newestCookie) > 0
    ) {
      newestCookie = cookieJSON;
      bestSnapshot = clientGroupSnapshotCommit;
    }
  }

  if (bestSnapshot) {
    return {
      type: FIND_MATCHING_CLIENT_TYPE_FORK,
      snapshot: bestSnapshot,
    };
  }

  return {type: FIND_MATCHING_CLIENT_TYPE_NEW};
}

function getRefsForClients(clients: ClientMap): Hash[] {
  const refs: Hash[] = [];
  for (const client of clients.values()) {
    refs.push(client.headHash);
    if (isClientV5(client) && client.tempRefreshHash) {
      refs.push(client.tempRefreshHash);
    }
  }
  return refs;
}

export async function getClientGroupForClient(
  clientID: ClientID,
  read: dag.Read,
): Promise<ClientGroup | undefined> {
  const clientGroupID = await getClientGroupIDForClient(clientID, read);
  if (!clientGroupID) {
    return undefined;
  }
  return getClientGroup(clientGroupID, read);
}

export async function getClientGroupIDForClient(
  clientID: ClientID,
  read: dag.Read,
): Promise<sync.ClientGroupID | undefined> {
  const client = await getClient(clientID, read);
  if (!client || !isClientV5(client)) {
    return undefined;
  }
  return client.clientGroupID;
}

/**
 * Adds a Client to the ClientMap and updates the 'clients' head top point at
 * the updated clients.
 */
export async function setClient(
  clientID: ClientID,
  client: Client,
  dagWrite: dag.Write,
): Promise<Hash> {
  const clients = await getClients(dagWrite);
  const newClients = new Map(clients).set(clientID, client);
  return setClients(newClients, dagWrite);
}

/**
 * Sets the ClientMap and updates the 'clients' head top point at the new
 * clients.
 */
export async function setClients(
  clients: ClientMap,
  dagWrite: dag.Write,
): Promise<Hash> {
  const chunkData = clientMapToChunkData(clients, dagWrite);
  const chunk = dagWrite.createChunk(chunkData, getRefsForClients(clients));
  await dagWrite.putChunk(chunk);
  await dagWrite.setHead(CLIENTS_HEAD_NAME, chunk.hash);
  return chunk.hash;
}
