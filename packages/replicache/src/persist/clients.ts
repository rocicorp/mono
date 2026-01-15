import type {LogContext} from '@rocicorp/logger';
import {assert, assertObject} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import {hasOwn} from '../../../shared/src/has-own.ts';
import * as valita from '../../../shared/src/valita.ts';
import {emptyDataNode} from '../btree/node.ts';
import {BTreeRead} from '../btree/read.ts';
import {type FrozenCookie, compareCookies} from '../cookies.ts';
import {type Refs, toRefs} from '../dag/chunk.ts';
import type {Read, Store, Write} from '../dag/store.ts';
import type {Commit} from '../db/commit.ts';
import {
  type ChunkIndexDefinition,
  type IndexRecord,
  type SnapshotMetaDD31,
  assertSnapshotCommitDD31,
  baseSnapshotFromHash,
  chunkIndexDefinitionEqualIgnoreName,
  getRefs,
  newSnapshotCommitDataDD31,
  toChunkIndexDefinition,
} from '../db/commit.ts';
import {createIndexBTree} from '../db/write.ts';
import type {DeletedClients} from '../deleted-clients.ts';
import type * as FormatVersion from '../format-version-enum.ts';
import {type FrozenJSONValue, deepFreeze} from '../frozen-json.ts';
import {type Hash, hashSchema} from '../hash.ts';
import {type IndexDefinitions, indexDefinitionsEqual} from '../index-defs.ts';
import {
  type ClientGroupID,
  type ClientID,
  clientGroupIDSchema,
} from '../sync/ids.ts';
import {withWrite} from '../with-transactions.ts';
import {
  type ClientGroup,
  getClientGroup,
  getClientGroups,
  mutatorNamesEqual,
  setClientGroup,
} from './client-groups.ts';
import {makeClientID} from './make-client-id.ts';

type FormatVersion = Enum<typeof FormatVersion>;

export type ClientMap = ReadonlyMap<ClientID, ClientV5 | ClientV6>;

const clientV5Schema = valita.readonlyObject({
  heartbeatTimestampMs: valita.number(),

  headHash: hashSchema,

  /**
   * The hash of a commit we are in the middle of refreshing into this client's
   * memdag.
   */
  tempRefreshHash: hashSchema.nullable(),

  /**
   * ID of this client's perdag client group. This needs to be sent in pull
   * request (to enable syncing all last mutation ids in the client group).
   */
  clientGroupID: clientGroupIDSchema,
});

export type ClientV5 = valita.Infer<typeof clientV5Schema>;

const clientV6Schema = valita.readonlyObject({
  heartbeatTimestampMs: valita.number(),

  /**
   * A set of hashes, which contains:
   * 1. The hash of the last commit this client refreshed from its client group
   *    (this is the commit it bootstrapped from until it completes its first
   *    refresh).
   * 2. One or more hashes that were added to retain chunks of a commit while it
   *    was being refreshed into this client's memdag. (This can be one or more
   *    because refresh's cleanup step is a separate transaction and can fail).
   * Upon refresh completing and successfully running its clean up step, this
   * set will contain a single hash: the hash of the last commit this client
   * refreshed.
   */
  refreshHashes: valita.readonlyArray(hashSchema),

  /**
   * The hash of the last snapshot commit persisted by this client to this
   * client's client group, or null if has never persisted a snapshot.
   */
  persistHash: hashSchema.nullable(),

  /**
   * ID of this client's perdag client group. This needs to be sent in pull
   * request (to enable syncing all last mutation ids in the client group).
   */
  clientGroupID: clientGroupIDSchema,
});

export type ClientV6 = valita.Infer<typeof clientV6Schema>;

export type Client = ClientV5 | ClientV6;

function isClientV6(client: Client): client is ClientV6 {
  return (client as ClientV6).refreshHashes !== undefined;
}

export const CLIENTS_HEAD_NAME = 'clients';

const clientSchema = valita.union(clientV5Schema, clientV6Schema);

function assertClient(value: unknown): asserts value is Client {
  valita.assert(value, clientSchema);
}

export function assertClientV6(value: unknown): asserts value is ClientV6 {
  valita.assert(value, clientV6Schema);
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
  dagWrite: Write,
): FrozenJSONValue {
  for (const client of clients.values()) {
    if (isClientV6(client)) {
      client.refreshHashes.forEach(dagWrite.assertValidHash);
      if (client.persistHash) {
        dagWrite.assertValidHash(client.persistHash);
      }
    } else {
      dagWrite.assertValidHash(client.headHash);
      if (client.tempRefreshHash) {
        dagWrite.assertValidHash(client.tempRefreshHash);
      }
    }
  }
  return deepFreeze(Object.fromEntries(clients));
}

export async function getClients(dagRead: Read): Promise<ClientMap> {
  const hash = await dagRead.getHead(CLIENTS_HEAD_NAME);
  return getClientsAtHash(hash, dagRead);
}

async function getClientsAtHash(
  hash: Hash | undefined,
  dagRead: Read,
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
  constructor(id: ClientID) {
    super(`Client state not found, id: ${id}`);
    this.id = id;
  }
}

/**
 * Throws a `ClientStateNotFoundError` if the client does not exist.
 */
export async function assertHasClientState(
  id: ClientID,
  dagRead: Read,
): Promise<void> {
  if (!(await hasClientState(id, dagRead))) {
    throw new ClientStateNotFoundError(id);
  }
}

export async function hasClientState(
  id: ClientID,
  dagRead: Read,
): Promise<boolean> {
  return !!(await getClient(id, dagRead));
}

export async function getClient(
  id: ClientID,
  dagRead: Read,
): Promise<Client | undefined> {
  const clients = await getClients(dagRead);
  return clients.get(id);
}

export async function mustGetClient(
  id: ClientID,
  dagRead: Read,
): Promise<Client> {
  const client = await getClient(id, dagRead);
  if (!client) {
    throw new ClientStateNotFoundError(id);
  }
  return client;
}

type InitClientV6Result = [
  client: ClientV6,
  hash: Hash,
  clientMap: ClientMap,
  newClientGroup: boolean,
];

export function initClientV6(
  newClientID: ClientID,
  lc: LogContext,
  perdag: Store,
  mutatorNames: string[],
  indexes: IndexDefinitions,
  formatVersion: FormatVersion,
  enableClientGroupForking: boolean,
): Promise<InitClientV6Result> {
  return withWrite(perdag, async dagWrite => {
    async function setClientsAndClientGroupAndCommit(
      basisHash: Hash | null,
      cookieJSON: FrozenCookie,
      valueHash: Hash,
      indexRecords: readonly IndexRecord[],
    ): Promise<InitClientV6Result> {
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

      const newClientGroupID = makeClientID();

      const newClient: ClientV6 = {
        heartbeatTimestampMs: Date.now(),
        refreshHashes: [chunk.hash],
        persistHash: null,
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

      return [newClient, chunk.hash, newClients, true];
    }

    const clients = await getClients(dagWrite);

    const res = await findMatchingClient(dagWrite, mutatorNames, indexes);
    if (res.type === FIND_MATCHING_CLIENT_TYPE_HEAD) {
      // We found a client group with matching mutators and indexes. We can
      // reuse it.
      const {clientGroupID, headHash} = res;

      const newClient: ClientV6 = {
        clientGroupID,
        refreshHashes: [headHash],
        heartbeatTimestampMs: Date.now(),
        persistHash: null,
      };
      const newClients = new Map(clients).set(newClientID, newClient);
      await setClients(newClients, dagWrite);

      return [newClient, headHash, newClients, false];
    }

    if (
      !enableClientGroupForking ||
      res.type === FIND_MATCHING_CLIENT_TYPE_NEW
    ) {
      // No client group to fork from. Create empty snapshot.
      const emptyBTreeChunk = dagWrite.createChunk(emptyDataNode, []);
      await dagWrite.putChunk(emptyBTreeChunk);

      // Create indexes
      const indexRecords: IndexRecord[] = [];

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
    const indexRecords: IndexRecord[] = [];
    const {valueHash, indexes: oldIndexes} = snapshot;
    const map = new BTreeRead(dagWrite, formatVersion, valueHash);

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
          formatVersion,
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
  oldIndexes: readonly IndexRecord[],
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
      snapshot: Commit<SnapshotMetaDD31>;
    }
  | {
      type: typeof FIND_MATCHING_CLIENT_TYPE_HEAD;
      clientGroupID: ClientGroupID;
      headHash: Hash;
    };

export async function findMatchingClient(
  dagRead: Read,
  mutatorNames: string[],
  indexes: IndexDefinitions,
): Promise<FindMatchingClientResult> {
  let newestCookie: FrozenCookie | undefined;
  let bestSnapshot: Commit<SnapshotMetaDD31> | undefined;
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

    const clientGroupSnapshotCommit = await baseSnapshotFromHash(
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

function getRefsForClients(clients: ClientMap): Refs {
  const refs: Set<Hash> = new Set();
  for (const client of clients.values()) {
    if (isClientV6(client)) {
      for (const hash of client.refreshHashes) {
        refs.add(hash);
      }
      if (client.persistHash) {
        refs.add(client.persistHash);
      }
    } else {
      refs.add(client.headHash);
      if (client.tempRefreshHash) {
        refs.add(client.tempRefreshHash);
      }
    }
  }
  return toRefs(refs);
}

export async function getClientGroupForClient(
  clientID: ClientID,
  read: Read,
): Promise<ClientGroup | undefined> {
  const clientGroupID = await getClientGroupIDForClient(clientID, read);
  if (!clientGroupID) {
    return undefined;
  }
  return getClientGroup(clientGroupID, read);
}

export async function getClientGroupIDForClient(
  clientID: ClientID,
  read: Read,
): Promise<ClientGroupID | undefined> {
  const client = await getClient(clientID, read);
  return client?.clientGroupID;
}

/**
 * Adds a Client to the ClientMap and updates the 'clients' head to point at
 * the updated clients.
 */
export async function setClient(
  clientID: ClientID,
  client: Client,
  dagWrite: Write,
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
  dagWrite: Write,
): Promise<Hash> {
  const chunkData = clientMapToChunkData(clients, dagWrite);
  const chunk = dagWrite.createChunk(chunkData, getRefsForClients(clients));
  await dagWrite.putChunk(chunk);
  await dagWrite.setHead(CLIENTS_HEAD_NAME, chunk.hash);
  return chunk.hash;
}

/**
 * Callback function for when Replicache has deleted one or more clients.
 */
export type OnClientsDeleted = (
  deletedClients: DeletedClients,
) => Promise<void>;
