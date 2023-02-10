import type {LogContext} from '@rocicorp/logger';
import {assertHash, Hash, newUUIDHash} from '../hash';
import * as btree from '../btree/mod';
import * as dag from '../dag/mod';
import * as db from '../db/mod';
import type * as sync from '../sync/mod';
import type {ReadonlyJSONValue} from '../json';
import {assert, assertNumber, assertObject, assertString} from '../asserts';
import {hasOwn} from '../has-own';
import {uuid as makeUuid} from '../uuid';
import {
  assertSnapshotCommitDD31,
  compareCookies,
  getRefs,
  toChunkIndexDefinition,
  newSnapshotCommitData,
  newSnapshotCommitDataDD31,
  ChunkIndexDefinition,
  chunkIndexDefinitionEqualIgnoreName,
} from '../db/commit';
import type {ClientID} from '../sync/ids';
import {
  Branch,
  getBranch,
  getBranches,
  mutatorNamesEqual,
  setBranch,
} from './branches';
import {IndexDefinitions, indexDefinitionsEqual} from '../index-defs';
import {CastReason, InternalValue, safeCastToJSON} from '../internal-value';
import {createIndexBTree} from '../db/write';

export type ClientMap = ReadonlyMap<sync.ClientID, ClientSDD | ClientDD31>;

export type ClientSDD = {
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

export type ClientDD31 = {
  readonly heartbeatTimestampMs: number;
  readonly headHash: Hash;

  /**
   * The hash of a commit we are in the middle of refreshing into this client's
   * memdag.
   */
  readonly tempRefreshHash: Hash | null;

  /**
   * ID of this client's perdag branch. This needs to be sent in pull request
   * (to enable syncing all last mutation ids in the branch).
   */
  readonly branchID: sync.BranchID;
};

export type Client = ClientSDD | ClientDD31;

export function isClientDD31(client: Client): client is ClientDD31 {
  return DD31 && (client as ClientDD31).branchID !== undefined;
}

export function isClientSDD(client: Client): client is ClientSDD {
  return !DD31 || (client as ClientSDD).lastServerAckdMutationID !== undefined;
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
    assertString(value.branchID);
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

export function assertClientSDD(value: unknown): asserts value is ClientSDD {
  assertClientBase(value);
  const {mutationID, lastServerAckdMutationID} = value;
  assertNumber(mutationID);
  assertNumber(lastServerAckdMutationID);
}

export function assertClientDD31(value: unknown): asserts value is ClientDD31 {
  assert(DD31);
  assertClientBase(value);
  const {tempRefreshHash} = value;
  if (tempRefreshHash) {
    assertHash(tempRefreshHash);
  }
  assertString(value.branchID);
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
): ReadonlyJSONValue {
  for (const client of clients.values()) {
    dagWrite.assertValidHash(client.headHash);
    if (isClientDD31(client) && client.tempRefreshHash) {
      dagWrite.assertValidHash(client.tempRefreshHash);
    }
  }
  return Object.fromEntries(clients);
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

export function initClient(
  lc: LogContext,
  perdag: dag.Store,
  mutatorNames: string[],
  indexes: IndexDefinitions,
): Promise<[sync.ClientID, Client, ClientMap]> {
  if (DD31) {
    return initClientDD31(lc, perdag, mutatorNames, indexes);
  }

  return perdag.withWrite(async dagWrite => {
    const newClientID = makeUuid();
    const clients = await getClients(dagWrite);

    let bootstrapClient: Client | undefined;
    for (const client of clients.values()) {
      if (
        !bootstrapClient ||
        bootstrapClient.heartbeatTimestampMs < client.heartbeatTimestampMs
      ) {
        bootstrapClient = client;
      }
    }

    let newClientCommitData: db.CommitData<db.SnapshotMeta>;
    const chunksToPut = [];
    if (bootstrapClient) {
      const constBootstrapClient = bootstrapClient;
      const bootstrapCommit = await db.baseSnapshotFromHash(
        constBootstrapClient.headHash,
        dagWrite,
      );
      // Copy the snapshot with one change: set last mutation id to 0.  Replicache
      // server implementations expect new client ids to start with last mutation id 0.
      // If a server sees a new client id with a non-0 last mutation id, it may conclude
      // this is a very old client whose state has been garbage collected on the server.
      newClientCommitData = newSnapshotCommitData(
        bootstrapCommit.meta.basisHash,
        0 /* lastMutationID */,
        bootstrapCommit.meta.cookieJSON,
        bootstrapCommit.valueHash,
        bootstrapCommit.indexes,
      );
    } else {
      // No existing snapshot to bootstrap from. Create empty snapshot.
      const emptyBTreeChunk = dag.createChunkWithHash(
        newUUIDHash(),
        btree.emptyDataNode,
        [],
      );
      chunksToPut.push(emptyBTreeChunk);
      newClientCommitData = newSnapshotCommitData(
        null /* basisHash */,
        0 /* lastMutationID */,
        null /* cookie */,
        emptyBTreeChunk.hash,
        [] /* indexes */,
      );
    }

    const newClientCommitChunk = dag.createChunkWithHash(
      newUUIDHash(),
      newClientCommitData,
      getRefs(newClientCommitData),
    );
    chunksToPut.push(newClientCommitChunk);

    const newClient: Client = {
      heartbeatTimestampMs: Date.now(),
      headHash: newClientCommitChunk.hash,
      mutationID: 0,
      lastServerAckdMutationID: 0,
    };
    const updatedClients = new Map(clients).set(newClientID, newClient);
    await setClients(updatedClients, dagWrite);

    await Promise.all(chunksToPut.map(c => dagWrite.putChunk(c)));

    await dagWrite.commit();

    return [newClientID, newClient, updatedClients];
  });
}

export function initClientDD31(
  lc: LogContext,
  perdag: dag.Store,

  mutatorNames: string[],
  indexes: IndexDefinitions,
): Promise<[sync.ClientID, Client, ClientMap]> {
  assert(DD31);

  return perdag.withWrite(async dagWrite => {
    async function setClientsAndBranchAndCommit(
      basisHash: Hash | null,
      cookieJSON: InternalValue,
      valueHash: Hash,
      indexRecords: readonly db.IndexRecord[],
    ): Promise<[sync.ClientID, Client, ClientMap]> {
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

      const newBranchID = makeUuid();

      const newClient: ClientDD31 = {
        heartbeatTimestampMs: Date.now(),
        headHash: chunk.hash,
        tempRefreshHash: null,
        branchID: newBranchID,
      };

      const newClients = new Map(clients).set(newClientID, newClient);

      const branch: Branch = {
        headHash: chunk.hash,
        mutatorNames,
        indexes,
        mutationIDs: {},
        lastServerAckdMutationIDs: {},
      };

      await Promise.all([
        dagWrite.putChunk(chunk),
        setClients(newClients, dagWrite),
        setBranch(newBranchID, branch, dagWrite),
      ]);

      await dagWrite.commit();

      return [newClientID, newClient, newClients];
    }

    const newClientID = makeUuid();
    const clients = await getClients(dagWrite);

    const res = await findMatchingClient(dagWrite, mutatorNames, indexes);
    if (res.type === FIND_MATCHING_CLIENT_TYPE_HEAD) {
      // We found a branch with matching mutators and indexes. We can reuse it.
      const {branchID, headHash} = res;

      const newClient: ClientDD31 = {
        branchID,
        headHash,
        heartbeatTimestampMs: Date.now(),
        tempRefreshHash: null,
      };
      const newClients = new Map(clients).set(newClientID, newClient);
      await setClients(newClients, dagWrite);

      await dagWrite.commit();
      return [newClientID, newClient, newClients];
    }

    if (res.type === FIND_MATCHING_CLIENT_TYPE_NEW) {
      // No branch to fork from. Create empty snapshot.
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

      return setClientsAndBranchAndCommit(
        null,
        null,
        emptyBTreeChunk.hash,
        indexRecords,
      );
    }

    // Now we create a new client and branch that we fork from the found snapshot.
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

    return setClientsAndBranchAndCommit(
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
      branchID: sync.BranchID;
      headHash: Hash;
    };

export async function findMatchingClient(
  dagRead: dag.Read,
  mutatorNames: string[],
  indexes: IndexDefinitions,
): Promise<FindMatchingClientResult> {
  let newestCookie: ReadonlyJSONValue | undefined;
  let bestSnapshot: db.Commit<db.SnapshotMetaDD31> | undefined;
  const mutatorNamesSet = new Set(mutatorNames);

  const branches = await getBranches(dagRead);
  for (const [branchID, branch] of branches) {
    if (
      mutatorNamesEqual(mutatorNamesSet, branch.mutatorNames) &&
      indexDefinitionsEqual(indexes, branch.indexes)
    ) {
      // exact match
      return {
        type: FIND_MATCHING_CLIENT_TYPE_HEAD,
        branchID,
        headHash: branch.headHash,
      };
    }

    const branchSnapshotCommit = await db.baseSnapshotFromHash(
      branch.headHash,
      dagRead,
    );
    assertSnapshotCommitDD31(branchSnapshotCommit);

    const cookieJSON = safeCastToJSON(
      branchSnapshotCommit.meta.cookieJSON,
      CastReason.CompareCookies,
    );
    if (
      newestCookie === undefined ||
      compareCookies(cookieJSON, newestCookie) > 0
    ) {
      newestCookie = cookieJSON;
      bestSnapshot = branchSnapshotCommit;
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
    if (DD31 && isClientDD31(client) && client.tempRefreshHash) {
      refs.push(client.tempRefreshHash);
    }
  }
  return refs;
}

export async function getMainBranch(
  clientID: ClientID,
  read: dag.Read,
): Promise<Branch | undefined> {
  assert(DD31);
  const branchID = await getMainBranchID(clientID, read);
  if (!branchID) {
    return undefined;
  }
  return await getBranch(branchID, read);
}

export async function getMainBranchID(
  clientID: ClientID,
  read: dag.Read,
): Promise<sync.BranchID | undefined> {
  assert(DD31);
  const client = await getClient(clientID, read);
  if (!client || !isClientDD31(client)) {
    return undefined;
  }
  return client.branchID;
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
