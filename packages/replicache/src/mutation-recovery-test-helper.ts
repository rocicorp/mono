import {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import type {ReadonlyJSONObject} from 'shared/src/json.js';
import * as dag from './dag/mod.js';
import {assertLocalMetaDD31} from './db/commit.js';
import type * as db from './db/mod.js';
import {ChainBuilder} from './db/test-helpers.js';
import {FormatVersion} from './format-version.js';
import {assertHash} from './hash.js';
import * as kv from './kv/mod.js';
import {initClientWithClientID} from './persist/clients-test-helpers.js';
import * as persist from './persist/mod.js';
import {persistSDD} from './persist/persist-test-helpers.js';
import type {MutatorDefs} from './replicache.js';
import {makeIDBNameForTesting} from './replicache.js';
import type {ClientGroupID, ClientID} from './sync/ids.js';
import {PUSH_VERSION_DD31, PUSH_VERSION_SDD} from './sync/push.js';
import {closeablesToClose, dbsToDrop} from './test-util.js';
import {uuid} from './uuid.js';

export async function createPerdag(args: {
  replicacheName: string;
  schemaVersion: string;
  formatVersion: FormatVersion;
}): Promise<dag.Store> {
  const {replicacheName, schemaVersion, formatVersion: formatVersion} = args;
  const idbName = makeIDBNameForTesting(
    replicacheName,
    schemaVersion,
    formatVersion,
  );
  const idb = new kv.IDBStore(idbName);
  closeablesToClose.add(idb);
  dbsToDrop.add(idbName);

  const createKVStore = (name: string) => new kv.IDBStore(name);
  const idbDatabases = new persist.IDBDatabasesStore(createKVStore);
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
  const perdag = new dag.StoreImpl(idb, dag.uuidChunkHasher, assertHash);
  return perdag;
}

export async function createAndPersistClientWithPendingLocalSDD(
  clientID: ClientID,
  perdag: dag.Store,
  numLocal: number,
): Promise<db.LocalMetaSDD[]> {
  const formatVersion = FormatVersion.SDD;
  const testMemdag = new dag.LazyStore(
    perdag,
    100 * 2 ** 20,
    dag.uuidChunkHasher,
    assertHash,
  );
  const b = new ChainBuilder(testMemdag, undefined, formatVersion);
  await b.addGenesis(clientID);
  await b.addSnapshot([['unique', uuid()]], clientID);

  await initClientWithClientID(clientID, perdag, [], {}, formatVersion);

  const localMetas: db.LocalMetaSDD[] = [];
  for (let i = 0; i < numLocal; i++) {
    await b.addLocal(clientID);
    localMetas.push(b.chain[b.chain.length - 1].meta as db.LocalMetaSDD);
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
  perdag: dag.Store;
  numLocal: number;
  mutatorNames: string[];
  cookie: string | number;
  formatVersion: FormatVersion;
  snapshotLastMutationIDs?: Record<ClientID, number> | undefined;
}): Promise<db.LocalMetaDD31[]> {
  assert(formatVersion >= FormatVersion.DD31);
  const testMemdag = new dag.LazyStore(
    perdag,
    100 * 2 ** 20, // 100 MB,
    dag.uuidChunkHasher,
    assertHash,
  );

  const b = new ChainBuilder(testMemdag, undefined, formatVersion);

  await b.addGenesis(clientID);
  await b.addSnapshot(
    [['unique', uuid()]],
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

  const localMetas: db.LocalMetaDD31[] = [];
  for (let i = 0; i < numLocal; i++) {
    await b.addLocal(clientID);
    const {meta} = b.chain[b.chain.length - 1];
    assertLocalMetaDD31(meta);
    localMetas.push(meta);
  }

  const mutators: MutatorDefs = Object.fromEntries(
    mutatorNames.map(n => [n, () => Promise.resolve()]),
  );

  await persist.persistDD31(
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
  perdag: dag.Store,
  cookie: string | number,
  mutatorNames: string[],
  snapshotLastMutationIDs: Record<ClientID, number>,
  formatVersion: FormatVersion,
): Promise<void> {
  const testMemdag = new dag.LazyStore(
    perdag,
    100 * 2 ** 20, // 100 MB,
    dag.uuidChunkHasher,
    assertHash,
  );

  const b = new ChainBuilder(testMemdag, undefined, FormatVersion.Latest);

  await b.addGenesis(clientID);
  await b.addSnapshot(
    [['unique', uuid()]],
    clientID,
    cookie,
    snapshotLastMutationIDs,
  );

  const mutators: MutatorDefs = Object.fromEntries(
    mutatorNames.map(n => [n, () => Promise.resolve()]),
  );

  await persist.persistDD31(
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
  localMetas: db.LocalMetaDD31[],
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
  localMetas: db.LocalMetaSDD[],
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
