import {afterEach, describe, expect, test, vi} from 'vitest';
import {assert, assertNotUndefined} from '../../shared/src/asserts.ts';
import {sleep} from '../../shared/src/sleep.ts';
import {StoreImpl} from './dag/store-impl.ts';
import type {Store} from './dag/store.ts';
import {assertHash, newRandomHash} from './hash.ts';
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
  ClientStateNotFoundError,
  getClient,
} from './persist/clients.ts';
import type {ReplicacheTest} from './test-util.ts';
import {
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

async function deleteClientGroupForTesting<MD extends MutatorDefs = {}>(
  rep: ReplicacheTest<MD>,
) {
  const clientGroupID = await rep.clientGroupID;
  assert(clientGroupID);
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
