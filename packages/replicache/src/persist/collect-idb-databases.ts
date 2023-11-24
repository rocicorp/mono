import {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import {initBgIntervalProcess} from '../bg-interval.js';
import {uuidChunkHasher} from '../dag/chunk.js';
import {StoreImpl} from '../dag/store-impl.js';
import type {Store} from '../dag/store.js';
import {FormatVersion} from '../format-version.js';
import {assertHash} from '../hash.js';
import {newIDBStoreWithMemFallback} from '../kv/idb-store-with-mem-fallback.js';
import {IDBStore} from '../kv/idb-store.js';
import {dropStore} from '../kv/idb-util.js';
import type {CreateStore} from '../kv/store.js';
import type {ClientGroupID, ClientID} from '../sync/ids.js';
import {withRead} from '../with-transactions.js';
import {
  clientGroupHasPendingMutations,
  getClientGroups,
} from './client-groups.js';
import {ClientMap, getClients, isClientV4} from './clients.js';
import type {IndexedDBDatabase} from './idb-databases-store.js';
import {IDBDatabasesStore} from './idb-databases-store.js';

// How frequently to try to collect
const COLLECT_INTERVAL_MS = 12 * 60 * 60 * 1000; // 12 hours

// If an IDB database is older than MAX_AGE, then it can be collected.
const MAX_AGE = 30 * 24 * 60 * 60 * 1000; // 1 month

// If an IDB database is older than DD31_MAX_AGE **and** has no pending
// mutations, then it can be collected.
const DD31_MAX_AGE = 14 * 24 * 60 * 60 * 1000; // 2 weeks

// We delay the initial collection to prevent doing it at startup.
const COLLECT_DELAY = 5 * 60 * 1000; // 5 minutes

export function initCollectIDBDatabases(
  idbDatabasesStore: IDBDatabasesStore,
  onClientsRemoved: (clientID: Set<ClientID>) => void,
  lc: LogContext,
  signal: AbortSignal,
): void {
  let initial = true;
  initBgIntervalProcess(
    'CollectIDBDatabases',
    async () => {
      await collectIDBDatabases(
        idbDatabasesStore,
        onClientsRemoved,
        Date.now(),
        MAX_AGE,
        DD31_MAX_AGE,
      );
    },
    () => {
      if (initial) {
        initial = false;
        return COLLECT_DELAY;
      }
      return COLLECT_INTERVAL_MS;
    },
    lc,
    signal,
  );
}

export async function collectIDBDatabases(
  idbDatabasesStore: IDBDatabasesStore,
  onClientsRemoved: (clientIDs: Set<ClientID>) => void,
  now: number,
  maxAge: number,
  dd31MaxAge: number,
  newDagStore = defaultNewDagStore,
): Promise<void> {
  const databases = await idbDatabasesStore.getDatabases();

  const clientIDsToRemove = new Set<ClientID>();
  const dbs = Object.values(databases) as IndexedDBDatabase[];
  const canCollectResults = await Promise.all(
    dbs.map(
      async db =>
        [
          db.name,
          await canCollectDatabase(
            db,
            now,
            maxAge,
            dd31MaxAge,
            newDagStore,
            clientIDsToRemove,
          ),
        ] as const,
    ),
  );

  const namesToRemove = canCollectResults
    .filter(result => result[1])
    .map(result => result[0]);

  const {errors} = await dropDatabases(idbDatabasesStore, namesToRemove);
  if (errors.length) {
    throw errors[0];
  }

  if (clientIDsToRemove.size > 0) {
    onClientsRemoved(clientIDsToRemove);
  }
}

async function dropDatabaseInternal(
  name: string,
  idbDatabasesStore: IDBDatabasesStore,
) {
  await dropStore(name);
  await idbDatabasesStore.deleteDatabases([name]);
}

async function dropDatabases(
  idbDatabasesStore: IDBDatabasesStore,
  namesToRemove: string[],
): Promise<{dropped: string[]; errors: unknown[]}> {
  // Try to remove the databases in parallel. Don't let a single reject fail the
  // other ones. We will check for failures afterwards.
  const dropStoreResults = await Promise.allSettled(
    namesToRemove.map(async name => {
      await dropDatabaseInternal(name, idbDatabasesStore);
      return name;
    }),
  );

  const dropped: string[] = [];
  const errors: unknown[] = [];
  for (const result of dropStoreResults) {
    if (result.status === 'fulfilled') {
      dropped.push(result.value);
    } else {
      errors.push(result.reason);
    }
  }

  return {dropped, errors};
}

function defaultNewDagStore(name: string): Store {
  const perKvStore = new IDBStore(name);
  return new StoreImpl(perKvStore, uuidChunkHasher, assertHash);
}

async function canCollectDatabase(
  db: IndexedDBDatabase,
  now: number,
  maxAge: number,
  dd31MaxAge: number,
  newDagStore: typeof defaultNewDagStore,
  clientIDsToRemove: Set<ClientID>,
): Promise<boolean> {
  if (db.replicacheFormatVersion > FormatVersion.Latest) {
    return false;
  }

  // 0 is used in testing
  if (db.lastOpenedTimestampMS !== undefined) {
    const isDD31 = db.replicacheFormatVersion >= FormatVersion.DD31;

    // - For SDD we can delete the database if it is older than maxAge.
    // - For DD31 we can delete the database if it is older than dd31MaxAge and
    //   there are no pending mutations.
    if (now - db.lastOpenedTimestampMS < (isDD31 ? dd31MaxAge : maxAge)) {
      return false;
    }

    if (!isDD31) {
      // Pre DD31 we do not care about the removed clients.
      return true;
    }

    // If increase the format version we need to decide how to deal with this
    // logic.
    assert(
      db.replicacheFormatVersion === FormatVersion.DD31 ||
        db.replicacheFormatVersion === FormatVersion.V6 ||
        db.replicacheFormatVersion === FormatVersion.V7,
    );
    return !(await anyPendingMutationsInClientGroups(
      newDagStore(db.name),
      clientIDsToRemove,
    ));
  }

  // For legacy databases we do not have a lastOpenedTimestampMS so we check the
  // time stamps of the clients
  const perdag = newDagStore(db.name);
  const clientMap = await withRead(perdag, getClients);
  await perdag.close();

  return allClientsOlderThan(clientMap, now, maxAge);
}

function allClientsOlderThan(
  clients: ClientMap,
  now: number,
  maxAge: number,
): boolean {
  for (const client of clients.values()) {
    if (now - client.heartbeatTimestampMs < maxAge) {
      return false;
    }
  }
  return true;
}

/**
 * Deletes a single Replicache database.
 * @param dbName
 * @param createKVStore
 */
export async function dropDatabase(
  dbName: string,
  createKVStore: CreateStore = name =>
    newIDBStoreWithMemFallback(new LogContext(), name),
) {
  await dropDatabaseInternal(dbName, new IDBDatabasesStore(createKVStore));
}

/**
 * Deletes all IndexedDB data associated with Replicache.
 *
 * Returns an object with the names of the successfully dropped databases
 * and any errors encountered while dropping.
 */
export async function dropAllDatabases(
  createKVStore: CreateStore = name =>
    newIDBStoreWithMemFallback(new LogContext(), name),
): Promise<{
  dropped: string[];
  errors: unknown[];
}> {
  const store = new IDBDatabasesStore(createKVStore);
  const databases = await store.getDatabases();
  const dbNames = Object.values(databases).map(db => db.name);

  const result = await dropDatabases(store, dbNames);
  return result;
}

/**
 * Deletes all IndexedDB data associated with Replicache.
 *
 * Returns an object with the names of the successfully dropped databases
 * and any errors encountered while dropping.
 *
 * @deprecated Use `dropAllDatabases` instead.
 */
export function deleteAllReplicacheData(createKVStore?: CreateStore) {
  return dropAllDatabases(createKVStore);
}

async function anyPendingMutationsInClientGroups(
  perdag: Store,
  clientIDsToRemove: Set<ClientID>,
): Promise<boolean> {
  const [clients, clientGroups] = await withRead(perdag, tx =>
    Promise.all([getClients(tx), getClientGroups(tx)]),
  );
  const clientGroupIDsToRemove = new Set<ClientGroupID>();
  for (const [clientGroupID, clientGroup] of clientGroups) {
    if (clientGroupHasPendingMutations(clientGroup)) {
      return true;
    }
    clientGroupIDsToRemove.add(clientGroupID);
  }

  for (const [clientID, client] of clients) {
    if (
      !isClientV4(client) &&
      (clientGroupIDsToRemove.has(client.clientGroupID) ||
        !clientGroups.has(client.clientGroupID))
    ) {
      clientIDsToRemove.add(clientID);
    }
  }
  return false;
}
