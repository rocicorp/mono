import {LogContext} from '@rocicorp/logger';
import {assert} from '../../shared/src/asserts.ts';
import type {Enum} from '../../shared/src/enum.ts';
import type {ReadonlyJSONObject} from '../../shared/src/json.ts';
import {LazyStore} from './dag/lazy-store.ts';
import {StoreImpl} from './dag/store-impl.ts';
import type {Store} from './dag/store.ts';
import {type LocalMetaDD31, assertLocalMetaDD31} from './db/commit.ts';
import {ChainBuilder} from './db/test-helpers.ts';
import * as FormatVersion from './format-version-enum.ts';
import {assertHash, newRandomHash} from './hash.ts';
import {IDBStore} from './kv/idb-store.ts';
import {initClientWithClientID} from './persist/clients-test-helpers.ts';
import {IDBDatabasesStore} from './persist/idb-databases-store.ts';
import {persistDD31} from './persist/persist.ts';
import {makeIDBNameForTesting} from './replicache.ts';
import type {ClientGroupID, ClientID} from './sync/ids.ts';
import {PUSH_VERSION_ZERO} from './sync/push.ts';
import {closeablesToClose, dbsToDrop} from './test-util.ts';
import type {MutatorDefs} from './types.ts';

type FormatVersion = Enum<typeof FormatVersion>;

export async function createPerdag(args: {
  replicacheName: string;
  schemaVersion: string;
  formatVersion: FormatVersion;
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
  formatVersion: FormatVersion;
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
    undefined,
  );

  return localMetas;
}

export async function persistSnapshotDD31(
  clientID: ClientID,
  perdag: Store,
  cookie: string | number,
  mutatorNames: string[],
  snapshotLastMutationIDs: Record<ClientID, number>,
  formatVersion: FormatVersion,
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
    undefined,
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
    pushVersion: PUSH_VERSION_ZERO,
    schemaVersion,
  };
}
