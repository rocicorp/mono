export {persist} from './persist';
export {startHeartbeats} from './heartbeat';
export {
  initClient,
  getClient,
  getClients,
  updateClients,
  noUpdates as noClientUpdates,
  hasClientState,
  assertHasClientState,
  ClientStateNotFoundError,
} from './clients';
export {initClientGC} from './client-gc';
export {
  IDBDatabasesStore,
  setupForTest as setupIDBDatabasesStoreForTest,
  teardownForTest as teardownIDBDatabasesStoreForTest,
} from './idb-databases-store';

export type {Client, ClientMap} from './clients';
export type {
  IndexedDBDatabase,
  IndexedDBDatabaseRecord,
} from './idb-databases-store';
export {
  initCollectIDBDatabases,
  deleteAllReplicacheData,
} from './collect-idb-databases';
