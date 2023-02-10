export {persist} from './persist';
export {startHeartbeats} from './heartbeat';
export {getBranch, getBranches, setBranch, setBranches} from './branches';
export type {Branch, BranchMap} from './branches';
export {initBranchGC, gcBranches} from './branch-gc';
export {
  initClient,
  getClient,
  getClients,
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
