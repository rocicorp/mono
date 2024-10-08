import {LogContext} from '@rocicorp/logger';
import {assert} from '../../shared/src/asserts.js';
import type {ReadonlyJSONObject} from '../../shared/src/json.js';
import {LazyStore} from './dag/lazy-store.js';
import {StoreImpl} from './dag/store-impl.js';
import type {Store} from './dag/store.js';
import {
  type LocalMetaDD31,
  type LocalMetaSDD,
  assertLocalMetaDD31,
} from './db/commit.js';
import {ChainBuilder} from './db/test-helpers.js';
import * as FormatVersion from './format-version-enum.js';
import {assertHash, newRandomHash} from './hash.js';
import {IDBStore} from './kv/idb-store.js';
import {initClientWithClientID} from './persist/clients-test-helpers.js';
import {IDBDatabasesStore} from './persist/idb-databases-store.js';
import {persistSDD} from './persist/persist-test-helpers.js';
import {persistDD31} from './persist/persist.js';
import {makeIDBNameForTesting} from './replicache.js';
import type {ClientGroupID, ClientID} from './sync/ids.js';
import {PUSH_VERSION_DD31, PUSH_VERSION_SDD} from './sync/push.js';
import {closeablesToClose, dbsToDrop} from './test-util.js';
import type {MutatorDefs} from './types.js';

export async function createPerdag(args: {
  replicacheName: string;
  schemaVersion: string;
  formatVersion: FormatVersion.Type;
}): Promise<Store> {
  const {replicacheName, schemaVersion, formatVersion: formatVersion} = args;
  const idbName = makeIDBNameForTesting(
    replicacheName,
    schemaVersion,
    formatVersion,
  );
  const idb = new IDBStore(idbName);
  closeablesToClose.add(idb);
  dbsToDrop.add(idbName);

  const createKVStore = (name: string) => new IDBStore(name);
  const idbDatabases = new IDBDatabasesStore(createKVStore);
  try {
    await idbDatabases.putDatabase({
      name: idbName,
      replicacheName,
      schemaVersion,
      replicacheFormatVersion: formatVersion,
    });
  } finally {
    await idbDatabases.close();
  }
  const perdag = new StoreImpl(idb, newRandomHash, assertHash);
  return perdag;
}

export async function createAndPersistClientWithPendingLocalSDD(
  clientID: ClientID,
  perdag: Store,
  numLocal: number,
): Promise<LocalMetaSDD[]> {
  const formatVersion = FormatVersion.SDD;
  const testMemdag = new LazyStore(
    perdag,
    100 * 2 ** 20,
    newRandomHash,
    assertHash,
  );
  const b = new ChainBuilder(testMemdag, undefined, formatVersion);
  await b.addGenesis(clientID);
  await b.addSnapshot([['unique', Math.random()]], clientID);

  await initClientWithClientID(clientID, perdag, [], {}, formatVersion);

  const localMetas: LocalMetaSDD[] = [];
  for (let i = 0; i < numLocal; i++) {
    await b.addLocal(clientID);
    localMetas.push(b.chain[b.chain.length - 1].meta as LocalMetaSDD);
  }

  await persistSDD(clientID, testMemdag, perdag, () => false);
  return localMetas;
}

export async function createAndPersistClientWithPendingLocalDD31({
  clientID,
  perdag,
  numLocal,
  mutatorNames,
  cookie,
  formatVersion,
  snapshotLastMutationIDs,
}: {
  clientID: ClientID;
  perdag: Store;
  numLocal: number;
  mutatorNames: string[];
  cookie: string | number;
  formatVersion: FormatVersion.Type;
  snapshotLastMutationIDs?: Record<ClientID, number> | undefined;
}): Promise<LocalMetaDD31[]> {
  assert(formatVersion >= FormatVersion.DD31);
  const testMemdag = new LazyStore(
    perdag,
    100 * 2 ** 20, // 100 MB,
    newRandomHash,
    assertHash,
  );

  const b = new ChainBuilder(testMemdag, undefined, formatVersion);

  await b.addGenesis(clientID);
  await b.addSnapshot(
    [['unique', Math.random()]],
    clientID,
    cookie,
    snapshotLastMutationIDs,
  );

  await initClientWithClientID(
    clientID,
    perdag,
    mutatorNames,
    {},
    formatVersion,
  );

  const localMetas: LocalMetaDD31[] = [];
  for (let i = 0; i < numLocal; i++) {
    await b.addLocal(clientID);
    const {meta} = b.chain[b.chain.length - 1];
    assertLocalMetaDD31(meta);
    localMetas.push(meta);
  }

  const mutators: MutatorDefs = Object.fromEntries(
    mutatorNames.map(n => [n, () => Promise.resolve()]),
  );

  await persistDD31(
    new LogContext(),
    clientID,
    testMemdag,
    perdag,
    mutators,
    () => false,
    formatVersion,
  );

  return localMetas;
}

export async function persistSnapshotDD31(
  clientID: ClientID,
  perdag: Store,
  cookie: string | number,
  mutatorNames: string[],
  snapshotLastMutationIDs: Record<ClientID, number>,
  formatVersion: FormatVersion.Type,
): Promise<void> {
  const testMemdag = new LazyStore(
    perdag,
    100 * 2 ** 20, // 100 MB,
    newRandomHash,
    assertHash,
  );

  const b = new ChainBuilder(testMemdag, undefined, FormatVersion.Latest);

  await b.addGenesis(clientID);
  await b.addSnapshot(
    [['unique', Math.random()]],
    clientID,
    cookie,
    snapshotLastMutationIDs,
  );

  const mutators: MutatorDefs = Object.fromEntries(
    mutatorNames.map(n => [n, () => Promise.resolve()]),
  );

  await persistDD31(
    new LogContext(),
    clientID,
    testMemdag,
    perdag,
    mutators,
    () => false,
    formatVersion,
  );
}

export function createPushRequestBodyDD31(
  profileID: string,
  clientGroupID: ClientGroupID,
  clientID: ClientID,
  localMetas: LocalMetaDD31[],
  schemaVersion: string,
): ReadonlyJSONObject {
  return {
    profileID,
    clientGroupID,
    mutations: localMetas.map(localMeta => ({
      clientID,
      id: localMeta.mutationID,
      name: localMeta.mutatorName,
      args: localMeta.mutatorArgsJSON,
      timestamp: localMeta.timestamp,
    })),
    pushVersion: PUSH_VERSION_DD31,
    schemaVersion,
  };
}

export function createPushBodySDD(
  profileID: string,
  clientID: ClientID,
  localMetas: LocalMetaSDD[],
  schemaVersion: string,
): ReadonlyJSONObject {
  return {
    profileID,
    clientID,
    mutations: localMetas.map(localMeta => ({
      id: localMeta.mutationID,
      name: localMeta.mutatorName,
      args: localMeta.mutatorArgsJSON,
      timestamp: localMeta.timestamp,
    })),
    pushVersion: PUSH_VERSION_SDD,
    schemaVersion,
  };
}
