import {resolver} from '@rocicorp/resolver';
import {afterEach, describe, expect, test, vi} from 'vitest';
import {assert, assertNotUndefined} from '../../shared/src/asserts.ts';
import {sleep} from '../../shared/src/sleep.ts';
import {chunkRefCountKey} from './dag/key.ts';
import {StoreImpl, WriteImpl} from './dag/store-impl.ts';
import type {Store} from './dag/store.ts';
import {assertHash, type Hash, newRandomHash} from './hash.ts';
import {dropIDBStoreWithMemFallback} from './kv/idb-store-with-mem-fallback.ts';
import {IDBNotFoundError, IDBStore} from './kv/idb-store.ts';
import {
  type ClientGroup,
  deleteClientGroup,
  getClientGroup,
} from './persist/client-groups.ts';
import {deleteClientForTesting} from './persist/clients-test-helpers.ts';
import {
  assertClientV6,
  CLIENTS_HEAD_NAME,
  ClientStateNotFoundError,
  getClient,
} from './persist/clients.ts';
import {HEARTBEAT_INTERVAL} from './persist/heartbeat.ts';
import {IDBDatabasesStore} from './persist/idb-databases-store.ts';
import {
  ReplicacheTest,
  addData,
  disableAllBackgroundProcesses,
  expectLogContext,
  fetchMocker,
  initReplicacheTesting,
  makePullResponseV1,
  replicacheForTesting,
  tickAFewTimes,
} from './test-util.ts';
import type {WriteTransaction} from './transactions.ts';
import type {MutatorDefs} from './types.ts';
import {withRead, withWriteNoImplicitCommit} from './with-transactions.ts';

initReplicacheTesting();

let perdag: Store | undefined;
afterEach(async () => {
  await perdag?.close();
  vi.restoreAllMocks();
});

/**
 * Overwrites the ref count of the current `clients` chunk with an invalid
 * value. The next write that replaces the `clients` head (persist, a new
 * client starting, a heartbeat) decrements this ref count and trips on it.
 */
async function corruptClientsRefCountForTesting(perdag: Store): Promise<Hash> {
  const clientsHash = await withRead(perdag, read =>
    read.getHead(CLIENTS_HEAD_NAME),
  );
  assert(clientsHash, 'Expected clients head to be defined');
  await withWriteNoImplicitCommit(perdag, async dagWrite => {
    assert(dagWrite instanceof WriteImpl, 'Expected WriteImpl');
    await dagWrite.kvWrite.put(chunkRefCountKey(clientsHash), -1);
    await dagWrite.commit();
  });
  return clientsHash;
}

async function expectDatabaseDropped(idbName: string): Promise<void> {
  const idbDatabases = new IDBDatabasesStore(name => new IDBStore(name));
  expect(Object.keys(await idbDatabases.getDatabases())).not.toContain(idbName);
  await idbDatabases.close();
  if (indexedDB.databases) {
    // Firefox does not support indexedDB.databases
    const names = (await indexedDB.databases()).map(db => db.name);
    expect(names).not.toContain(idbName);
  }
}

async function deleteClientGroupForTesting<MD extends MutatorDefs = {}>(
  rep: ReplicacheTest<MD>,
) {
  const clientGroupID = await rep.clientGroupID;
  assert(clientGroupID, 'Expected clientGroupID to be defined');
  await withWriteNoImplicitCommit(rep.perdag, async tx => {
    await deleteClientGroup(clientGroupID, tx);
    await tx.commit();
  });
}

test('basic persist & load', async () => {
  const pullURL = 'https://diff.com/pull';
  const rep = await replicacheForTesting('persist-test', {
    pullURL,
  });
  const {clientID} = rep;
  perdag = new StoreImpl(new IDBStore(rep.idbName), newRandomHash, assertHash);

  const clientBeforePull = await withRead(perdag, read =>
    getClient(clientID, read),
  );
  assertNotUndefined(clientBeforePull);

  assertClientV6(clientBeforePull);
  const clientGroupBeforePull = await withRead(perdag, read =>
    getClientGroup(clientBeforePull.clientGroupID, read),
  );
  assertNotUndefined(clientGroupBeforePull);

  fetchMocker.postOnce(
    pullURL,
    makePullResponseV1(clientID, 2, [
      {
        op: 'put',
        key: 'a',
        value: 1,
      },
      {
        op: 'put',
        key: 'b',
        value: 2,
      },
    ]),
  );

  await rep.pull();

  // maxWaitAttempts * waitMs should be at least PERSIST_TIMEOUT
  // plus some buffer for the persist process to complete
  const maxWaitAttempts = 20;
  const waitMs = 100;
  let waitAttempt = 0;
  const run = true;
  while (run) {
    if (waitAttempt++ > maxWaitAttempts) {
      throw new Error(
        `Persist did not complete in ${maxWaitAttempts * waitMs} ms`,
      );
    }
    await tickAFewTimes(vi, waitMs);
    assertClientV6(clientBeforePull);
    assertNotUndefined(clientGroupBeforePull);
    const clientGroup: ClientGroup | undefined = await withRead(perdag, read =>
      getClientGroup(clientBeforePull.clientGroupID, read),
    );
    assertNotUndefined(clientGroup);
    if (clientGroupBeforePull.headHash !== clientGroup.headHash) {
      // persist has completed
      break;
    }
  }

  await rep.query(async tx => {
    expect(await tx.get('a')).toBe(1);
    expect(await tx.get('b')).toBe(2);
  });

  // If we create another instance it will lazy load the data from IDB
  const rep2 = await replicacheForTesting(
    rep.name,
    {
      pullURL,
    },
    undefined,
    {useUniqueName: false},
  );
  await rep2.query(async tx => {
    expect(await tx.get('a')).toBe(1);
    expect(await tx.get('b')).toBe(2);
  });

  expect(rep.clientID).not.toBe(rep2.clientID);

  await perdag.close();
});

describe('onClientStateNotFound', () => {
  test('Called in persist if collected', async () => {
    const consoleErrorStub = vi.spyOn(console, 'error');

    const rep = await replicacheForTesting('called-in-persist', {
      mutators: {addData},
    });

    await rep.mutate.addData({foo: 'bar'});
    await rep.persist();

    const {clientID} = rep;
    await deleteClientForTesting(clientID, rep.perdag);

    const onClientStateNotFound = vi.fn();
    rep.onClientStateNotFound = onClientStateNotFound;
    await rep.persist();

    expect(onClientStateNotFound).toHaveBeenCalledTimes(1);
    expect(onClientStateNotFound.mock.lastCall).toEqual([]);
    expectLogContext(
      consoleErrorStub,
      0,
      rep,
      `Client state not found on client, clientID: ${clientID}`,
    );
  });

  test('Called in persist if the perdag has an invalid ref count', async () => {
    const consoleErrorStub = vi.spyOn(console, 'error');
    const pullURL = 'https://diff.com/pull';

    const rep = await replicacheForTesting(
      'called-in-persist-invalid-ref',
      {
        pullURL,
        mutators: {addData},
      },
      disableAllBackgroundProcesses,
    );

    await rep.mutate.addData({foo: 'bar'});
    await rep.persist();

    // Pull a newer snapshot so that the next persist writes it to the perdag
    // and rewrites the clients chunk.
    fetchMocker.postOnce(
      pullURL,
      makePullResponseV1(rep.clientID, 1, [{op: 'put', key: 'a', value: 1}]),
    );
    await rep.pull();

    const clientsHash = await corruptClientsRefCountForTesting(rep.perdag);

    const onClientStateNotFound = vi.fn();
    rep.onClientStateNotFound = onClientStateNotFound;
    await rep.persist();

    expect(onClientStateNotFound).toHaveBeenCalledTimes(1);
    expect(onClientStateNotFound.mock.lastCall).toEqual([]);

    // Disabling the client group would not be enough since the corrupt ref
    // count stays in the kv store. The whole database is dropped so that the
    // reload starts from a fresh store.
    await expectDatabaseDropped(rep.idbName);

    expect(consoleErrorStub.mock.calls.length).toBeGreaterThan(0);
    const [context, message, error] = consoleErrorStub.mock.calls[0];
    expect(context).toBe(`name=${rep.name}`);
    expect(message).toBe(
      `Client state is corrupt on client, clientID: ${rep.clientID}. Dropping database ${rep.idbName}`,
    );
    // The LogContext serializes errors to JSON before logging them.
    expect(error).toContain('"name":"InvalidRefCountError"');
    expect(error).toContain(`Invalid ref count -1 for ${clientsHash}`);
  });

  test('Called in open if the perdag has an invalid ref count', async () => {
    vi.spyOn(console, 'error');

    const rep = await replicacheForTesting(
      'called-in-open-invalid-ref',
      {mutators: {addData}},
      disableAllBackgroundProcesses,
    );
    await rep.mutate.addData({foo: 'bar'});
    await rep.persist();
    await corruptClientsRefCountForTesting(rep.perdag);
    await rep.close();

    // A new instance on the same database registers its client in the
    // `clients` chunk during open, before `ready` resolves. Without the
    // store-level hook that write would reject unhandled and the instance
    // would hang forever.
    const {promise: calledOnClientStateNotFound, resolve} = resolver();
    const rep2 = new ReplicacheTest(
      {name: rep.name, mutators: {addData}, pullURL: '', pushURL: ''},
      disableAllBackgroundProcesses,
    );
    rep2.onClientStateNotFound = resolve;
    await calledOnClientStateNotFound;

    expect(rep2.idbName).toBe(rep.idbName);
    await expectDatabaseDropped(rep.idbName);
  });

  test('Called from heartbeat if the perdag has an invalid ref count', async () => {
    vi.spyOn(console, 'error');

    const {promise: calledOnClientStateNotFound, resolve} = resolver();
    const onClientStateNotFound = vi.fn(resolve);
    const rep = await replicacheForTesting(
      'called-in-heartbeat-invalid-ref',
      {mutators: {addData}, onClientStateNotFound},
      disableAllBackgroundProcesses,
    );
    await rep.mutate.addData({foo: 'bar'});
    await rep.persist();
    await corruptClientsRefCountForTesting(rep.perdag);

    // The heartbeat rewrites the `clients` chunk, which decrements the corrupt
    // ref count. It runs as a background process, outside persist/refresh.
    await vi.advanceTimersByTimeAsync(HEARTBEAT_INTERVAL);
    await calledOnClientStateNotFound;

    expect(onClientStateNotFound).toHaveBeenCalledTimes(1);
    await expectDatabaseDropped(rep.idbName);
  });

  test('Called in query if collected', async () => {
    const consoleErrorStub = vi.spyOn(console, 'error');

    const name = 'called-in-query';
    const mutators = {
      addData,
    };
    const rep = await replicacheForTesting(
      name,
      {
        mutators,
      },
      disableAllBackgroundProcesses,
    );

    await rep.mutate.addData({foo: 'bar'});
    await rep.persist();
    const {clientID} = rep;
    await deleteClientForTesting(clientID, rep.perdag);

    // Need a real timeout here.
    vi.useRealTimers();
    await sleep(10);
    vi.useFakeTimers();

    await rep.close();

    const rep2 = await replicacheForTesting(
      rep.name,
      {
        mutators,
      },
      {
        // To ensure query has to go to perdag, prevent pull from happening and
        // populating the lazy store cache.
        enablePullAndPushInOpen: false,
        ...disableAllBackgroundProcesses,
      },
      // Use same idb and client group as above rep.
      {useUniqueName: false},
    );

    const {clientID: clientID2} = rep2;

    await deleteClientForTesting(clientID2, rep2.perdag);

    // Cannot simply gcClientGroups because the client group has pending mutations.
    await deleteClientGroupForTesting(rep2);

    const onClientStateNotFound = vi.fn();
    rep2.onClientStateNotFound = onClientStateNotFound;

    let e: unknown;
    try {
      await rep2.query(async tx => {
        await tx.get('foo');
      });
    } catch (err) {
      e = err;
    }
    expect(e).toBeInstanceOf(ClientStateNotFoundError);
    expectLogContext(
      consoleErrorStub,
      0,
      rep2,
      `Client state not found on client, clientID: ${clientID2}`,
    );
    expect(onClientStateNotFound.mock.lastCall).toEqual([]);
  });

  test('Called in mutate if collected', async () => {
    const consoleErrorStub = vi.spyOn(console, 'error');
    const name = 'called-in-mutate';
    const mutators = {
      addData,
      async check(tx: WriteTransaction, key: string) {
        await tx.has(key);
      },
    };

    const rep = await replicacheForTesting(
      name,
      {
        mutators,
      },
      disableAllBackgroundProcesses,
    );

    await rep.mutate.addData({foo: 'bar'});
    await rep.persist();
    const {clientID} = rep;
    await deleteClientForTesting(clientID, rep.perdag);
    await rep.close();

    const rep2 = await replicacheForTesting(
      rep.name,
      {
        mutators,
      },
      {
        ...disableAllBackgroundProcesses,
        // To ensure mutate has to go to perdag, prevent pull from happening and
        // populating the lazy store cache.
        enablePullAndPushInOpen: false,
      },
      // Use same idb and client group as above rep.
      {useUniqueName: false},
    );

    const {clientID: clientID2} = rep2;
    await deleteClientForTesting(clientID2, rep2.perdag);

    // Cannot simply gcClientGroups because the client group has pending mutations.
    await deleteClientGroupForTesting(rep2);

    const onClientStateNotFound = vi.fn();
    rep2.onClientStateNotFound = onClientStateNotFound;

    let e: unknown;
    try {
      // Another mutate will trigger
      await rep2.mutate.check('x');
    } catch (err) {
      e = err;
    }

    expect(e).toBeInstanceOf(ClientStateNotFoundError);
    expectLogContext(
      consoleErrorStub,
      0,
      rep2,
      `Client state not found on client, clientID: ${clientID2}`,
    );
    expect(onClientStateNotFound.mock.lastCall).toEqual([]);
  });
});

test('Persist throws if idb dropped', async () => {
  const rep = await replicacheForTesting(
    'called-in-persist-dropped',
    {
      mutators: {addData},
    },
    disableAllBackgroundProcesses,
    {useUniqueName: false},
  );

  await rep.mutate.addData({foo: 'bar'});

  await dropIDBStoreWithMemFallback(rep.idbName);

  const onClientStateNotFound = vi.fn();
  rep.onClientStateNotFound = onClientStateNotFound;
  let err;
  try {
    await rep.persist();
  } catch (e) {
    err = e;
  }
  expect(err).toBeInstanceOf(IDBNotFoundError);

  await rep.close();
});
