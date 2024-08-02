import type {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import {initBgIntervalProcess} from '../bg-interval.js';
import {uuidChunkHasher} from '../dag/chunk.js';
import {StoreImpl} from '../dag/store-impl.js';
import type {Store} from '../dag/store.js';
import {FormatVersion} from '../format-version.js';
import {assertHash} from '../hash.js';
import {IDBStore} from '../kv/idb-store.js';
import type {DropStore, StoreProvider} from '../kv/store.js';
import {createLogContext} from '../log-options.js';
import {getKVStoreProvider} from '../replicache.js';
import {withRead} from '../with-transactions.js';
import {
  clientGroupHasPendingMutations,
  getClientGroups,
} from './client-groups.js';
import {ClientMap, getClients} from './clients.js';
import type {IndexedDBDatabase} from './idb-databases-store.js';
import {IDBDatabasesStore} from './idb-databases-store.js';

/**
 * How frequently to try to collect
 */
export const COLLECT_IDB_INTERVAL = 12 * 60 * 60 * 1000; // 12 hours

/**
 * If an IDB database is older than MAX_AGE, then it can be collected.
 */
export const SDD_IDB_MAX_AGE = 30 * 24 * 60 * 60 * 1000; // 1 month

/**
 * If an IDB database is older than DD31_MAX_AGE **and** has no pending
 * mutations, then it can be collected.
 */
export const DD31_IDB_MAX_AGE = 14 * 24 * 60 * 60 * 1000; // 2 weeks

/**
 * We delay the initial collection to prevent doing it at startup.
 */
export const INITIAL_COLLECT_IDB_DELAY = 5 * 60 * 1000; // 5 minutes

export function initCollectIDBDatabases(
  idbDatabasesStore: IDBDatabasesStore,
  kvDropStore: DropStore,
  collectInterval: number,
  initialCollectDelay: number,
  sddMaxAge: number,
  dd31MaxAge: number,
  lc: LogContext,
  signal: AbortSignal,
): void {
  let initial = true;
  initBgIntervalProcess(
    'CollectIDBDatabases',
    async () => {
      await collectIDBDatabases(
        idbDatabasesStore,
        Date.now(),
        sddMaxAge,
        dd31MaxAge,
        kvDropStore,
      );
    },
    () => {
      if (initial) {
        initial = false;
        return initialCollectDelay;
      }
      return collectInterval;
    },
    lc,
    signal,
  );
}

/**
 * Collects IDB databases that are no longer needed.
 */
export async function collectIDBDatabases(
  idbDatabasesStore: IDBDatabasesStore,
  now: number,
  sddMaxAge: number,
  dd31MaxAge: number,
  kvDropStore: DropStore,
  newDagStore = defaultNewDagStore,
): Promise<void> {
  const databases = await idbDatabasesStore.getDatabases();

  const dbs = Object.values(databases) as IndexedDBDatabase[];
  const canCollectResults = await Promise.all(
    dbs.map(
      async db =>
        [
          db.name,
          await canCollectDatabase(db, now, sddMaxAge, dd31MaxAge, newDagStore),
        ] as const,
    ),
  );

  const namesToRemove = canCollectResults
    .filter(result => result[1])
    .map(result => result[0]);

  const {errors} = await dropDatabases(
    idbDatabasesStore,
    namesToRemove,
    kvDropStore,
  );
  if (errors.length) {
    throw errors[0];
  }
}

async function dropDatabaseInternal(
  name: string,
  idbDatabasesStore: IDBDatabasesStore,
  kvDropStore: DropStore,
) {
  await kvDropStore(name);
  await idbDatabasesStore.deleteDatabases([name]);
}

async function dropDatabases(
  idbDatabasesStore: IDBDatabasesStore,
  namesToRemove: string[],
  kvDropStore: DropStore,
): Promise<{dropped: string[]; errors: unknown[]}> {
  // Try to remove the databases in parallel. Don't let a single reject fail the
  // other ones. We will check for failures afterwards.
  const dropStoreResults = await Promise.allSettled(
    namesToRemove.map(async name => {
      await dropDatabaseInternal(name, idbDatabasesStore, kvDropStore);
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
  sddMaxAge: number,
  dd31MaxAge: number,
  newDagStore: typeof defaultNewDagStore,
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
    if (now - db.lastOpenedTimestampMS < (isDD31 ? dd31MaxAge : sddMaxAge)) {
      return false;
    }

    if (!isDD31) {
      return true;
    }

    // If increase the format version we need to decide how to deal with this
    // logic.
    assert(
      db.replicacheFormatVersion === FormatVersion.DD31 ||
        db.replicacheFormatVersion === FormatVersion.V6 ||
        db.replicacheFormatVersion === FormatVersion.V7,
    );
    return !(await anyPendingMutationsInClientGroups(newDagStore(db.name)));
  }

  // For legacy databases we do not have a lastOpenedTimestampMS so we check the
  // time stamps of the clients
  const perdag = newDagStore(db.name);
  const clientMap = await withRead(perdag, getClients);
  await perdag.close();

  return allClientsOlderThan(clientMap, now, sddMaxAge);
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
 * Options for `dropDatabase` and `dropAllDatabases`.
 */
export type DropDatabaseOptions = {
  /**
   * Allows providing a custom implementation of the underlying storage layer.
   * Default is `'idb'`.
   */
  kvStore?: 'idb' | 'mem' | StoreProvider | undefined;
  /**
   * Determines how much logging to do. When this is set to `'debug'`,
   * Replicache will also log `'info'` and `'error'` messages. When set to
   * `'info'` we log `'info'` and `'error'` but not `'debug'`. When set to
   * `'error'` we only log `'error'` messages.
   * Default is `'info'`.
   */
  logLevel?: LogLevel | undefined;
  /**
   * Enables custom handling of logs.
   *
   * By default logs are logged to the console.  If you would like logs to be
   * sent elsewhere (e.g. to a cloud logging service like DataDog) you can
   * provide an array of {@link LogSink}s.  Logs at or above
   * {@link DropDatabaseOptions.logLevel} are sent to each of these {@link LogSink}s.
   * If you would still like logs to go to the console, include
   * `consoleLogSink` in the array.
   *
   * ```ts
   * logSinks: [consoleLogSink, myCloudLogSink],
   * ```
   * Default is `[consoleLogSink]`.
   */
  logSinks?: LogSink[] | undefined;
};

/**
 * Deletes a single Replicache database.
 * @param dbName
 * @param createKVStore
 */

export async function dropDatabase(
  dbName: string,
  opts?: DropDatabaseOptions | undefined,
) {
  const logContext = createLogContext(opts?.logLevel, opts?.logSinks, {
    dropDatabase: undefined,
  });
  const kvStoreProvider = getKVStoreProvider(logContext, opts?.kvStore);
  await dropDatabaseInternal(
    dbName,
    new IDBDatabasesStore(kvStoreProvider.create),
    kvStoreProvider.drop,
  );
}

/**
 * Deletes all IndexedDB data associated with Replicache.
 *
 * Returns an object with the names of the successfully dropped databases
 * and any errors encountered while dropping.
 */
export async function dropAllDatabases(
  opts?: DropDatabaseOptions | undefined,
): Promise<{
  dropped: string[];
  errors: unknown[];
}> {
  const logContext = createLogContext(opts?.logLevel, opts?.logSinks, {
    dropAllDatabases: undefined,
  });
  const kvStoreProvider = getKVStoreProvider(logContext, opts?.kvStore);
  const store = new IDBDatabasesStore(kvStoreProvider.create);
  const databases = await store.getDatabases();
  const dbNames = Object.values(databases).map(db => db.name);
  const result = await dropDatabases(store, dbNames, kvStoreProvider.drop);
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
export function deleteAllReplicacheData(
  opts?: DropDatabaseOptions | undefined,
) {
  return dropAllDatabases(opts);
}

async function anyPendingMutationsInClientGroups(
  perdag: Store,
): Promise<boolean> {
  const clientGroups = await withRead(perdag, getClientGroups);
  for (const clientGroup of clientGroups.values()) {
    if (clientGroupHasPendingMutations(clientGroup)) {
      return true;
    }
  }
  return false;
}
