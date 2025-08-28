import type {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import type {MaybePromise} from '../../../shared/src/types.ts';
import {initBgIntervalProcess} from '../bg-interval.ts';
import {StoreImpl} from '../dag/store-impl.ts';
import type {Store} from '../dag/store.ts';
import {
  addDeletedClients,
  getDeletedClients,
  normalize,
} from '../deleted-clients.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {getKVStoreProvider} from '../get-kv-store-provider.ts';
import {assertHash, newRandomHash} from '../hash.ts';
import {IDBStore} from '../kv/idb-store.ts';
import type {DropStore, StoreProvider} from '../kv/store.ts';
import {createLogContext} from '../log-options.ts';
import type {ClientGroupID, ClientID} from '../sync/ids.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {
  clientGroupHasPendingMutations,
  getClientGroups,
} from './client-groups.ts';
import type {OnClientsDeleted} from './clients.ts';
import {getClients} from './clients.ts';
import type {IndexedDBDatabase} from './idb-databases-store.ts';
import {IDBDatabasesStore} from './idb-databases-store.ts';

/**
 * How frequently to try to collect
 */
export const COLLECT_IDB_INTERVAL = 12 * 60 * 60 * 1000; // 12 hours

/**
 * We delay the initial collection to prevent doing it at startup.
 */
export const INITIAL_COLLECT_IDB_DELAY = 5 * 60 * 1000; // 5 minutes

export function initCollectIDBDatabases(
  idbDatabasesStore: IDBDatabasesStore,
  kvDropStore: DropStore,
  collectInterval: number,
  initialCollectDelay: number,
  maxAge: number,
  enableMutationRecovery: boolean,
  onClientsDeleted: OnClientsDeleted,
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
        maxAge,
        kvDropStore,
        enableMutationRecovery,
        onClientsDeleted,
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
  maxAge: number,
  kvDropStore: DropStore,
  enableMutationRecovery: boolean,
  onClientsDeleted: OnClientsDeleted,
  newDagStore = defaultNewDagStore,
): Promise<void> {
  const databases = await idbDatabasesStore.getDatabases();

  const dbs = Object.values(databases) as IndexedDBDatabase[];
  const collectResults = await Promise.all(
    dbs.map(
      async db =>
        [
          db.name,
          await gatherDatabaseInfoForCollect(
            db,
            now,
            maxAge,
            enableMutationRecovery,
            newDagStore,
          ),
        ] as const,
    ),
  );

  const dbNamesToRemove: string[] = [];
  const dbNamesToKeep: string[] = [];
  const clientIDsToRemove: ClientID[] = [];
  const clientGroupIDsToRemove: ClientGroupID[] = [];
  for (const [
    dbName,
    [canCollect, clientIDs, clientGroupIDs],
  ] of collectResults) {
    if (canCollect) {
      dbNamesToRemove.push(dbName);
      clientIDsToRemove.push(...clientIDs);
      clientGroupIDsToRemove.push(...clientGroupIDs);
    } else {
      dbNamesToKeep.push(dbName);
    }
  }

  const {errors} = await dropDatabases(
    idbDatabasesStore,
    dbNamesToRemove,
    kvDropStore,
  );
  if (errors.length) {
    throw errors[0];
  }

  if (clientIDsToRemove.length || clientGroupIDsToRemove.length) {
    // Add the deleted clients to all the dbs that survived the collection.
    const newClientIDsToRemove: ClientID[] = clientIDsToRemove;
    const newClientGroupIDsToRemove: ClientGroupID[] = clientGroupIDsToRemove;
    for (const name of dbNamesToKeep) {
      await withWrite(newDagStore(name), async dagWrite => {
        const {clientIDs, clientGroupIDs} = await addDeletedClients(
          dagWrite,
          clientIDsToRemove,
          clientGroupIDsToRemove,
        );

        newClientIDsToRemove.push(...clientIDs);
        newClientGroupIDsToRemove.push(...clientGroupIDs);
      });
    }
    // normalize and dedupe
    onClientsDeleted(
      normalize(newClientIDsToRemove),
      normalize(newClientGroupIDsToRemove),
    );
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
  return new StoreImpl(perKvStore, newRandomHash, assertHash);
}

/**
 * If the database is older than maxAge and there are no pending mutations we
 * return `true` and an array of the clientIDs in that db. If the database is
 * too new or there are pending mutations we return `[false]`.
 */
function gatherDatabaseInfoForCollect(
  db: IndexedDBDatabase,
  now: number,
  maxAge: number,
  enableMutationRecovery: boolean,
  newDagStore: typeof defaultNewDagStore,
): MaybePromise<
  | [canCollect: false]
  | [
      canCollect: true,
      deletedClientIDs: ClientID[],
      deletedClientGroupIDs: ClientGroupID[],
    ]
> {
  if (db.replicacheFormatVersion > FormatVersion.Latest) {
    return [false];
  }

  // 0 is used in testing
  assert(db.lastOpenedTimestampMS !== undefined);

  // - For DD31 we can delete the database if it is older than maxAge and
  //   there are no pending mutations.
  if (now - db.lastOpenedTimestampMS < maxAge) {
    return [false];
  }
  // If increase the format version we need to decide how to deal with this
  // logic.
  assert(
    db.replicacheFormatVersion === FormatVersion.DD31 ||
      db.replicacheFormatVersion === FormatVersion.V6 ||
      db.replicacheFormatVersion === FormatVersion.V7,
  );
  return canDatabaseBeCollectedAndGetDeletedClientIDs(
    enableMutationRecovery,
    newDagStore(db.name),
  );
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
 * Drops the specified database.
 * @param dbName The name of the database to drop.
 * @param opts Options for dropping the database.
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

/**
 * If the there are pending mutations in any of the clients in this db we return
 * `[false]`. Otherwise we return `true` and an array of the clientIDs to
 * remove.
 */
function canDatabaseBeCollectedAndGetDeletedClientIDs(
  enableMutationRecovery: boolean,
  perdag: Store,
): Promise<
  | [canCollect: false]
  | [
      canCollect: true,
      deletedClientIDs: ClientID[],
      deletedClientGroupIDs: ClientGroupID[],
    ]
> {
  return withRead(perdag, async read => {
    // If mutation recovery is disabled we do not care if there are pending
    // mutations when we decide if we can collect the database.
    if (enableMutationRecovery) {
      const clientGroups = await getClientGroups(read);
      for (const clientGroup of clientGroups.values()) {
        if (clientGroupHasPendingMutations(clientGroup)) {
          return [false];
        }
      }
    }

    const clients = await getClients(read);
    const {clientIDs, clientGroupIDs} = await getDeletedClients(read);
    const newClientIDs: ClientID[] = [...clientIDs];
    const newClientGroupIDs: ClientGroupID[] = [...clientGroupIDs];
    for (const [clientID, client] of clients) {
      newClientIDs.push(clientID);
      newClientGroupIDs.push(client.clientGroupID);
    }
    // Deduping and sorting is done when storing
    return [true, newClientIDs, newClientGroupIDs];
  });
}
