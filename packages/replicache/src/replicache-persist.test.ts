import {
  addData,
  clock,
  disableAllBackgroundProcesses,
  expectLogContext,
  initReplicacheTesting,
  makePullResponseDD31,
  replicacheForTesting,
  ReplicacheTest,
  tickAFewTimes,
} from './test-util.js';
import {expect} from '@esm-bundle/chai';
import * as sinon from 'sinon';

// fetch-mock has invalid d.ts file so we removed that on npm install.
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-expect-error
import fetchMock from 'fetch-mock/esm/client';
import * as kv from './kv/mod.js';
import * as dag from './dag/mod.js';
import * as persist from './persist/mod.js';
import {assert, assertNotUndefined} from 'shared';
import {deleteClientForTesting} from './persist/clients-test-helpers.js';
import {assertClientDD31} from './persist/clients.js';
import {deleteClientGroup} from './persist/client-groups.js';
import {assertHash} from './hash.js';
import {IDBNotFoundError} from './kv/idb-store.js';
import type {WriteTransaction} from './transactions.js';
import type {MutatorDefs} from './replicache.js';
import {sleep} from './sleep.js';
import {withRead, withWrite} from './with-transactions.js';

initReplicacheTesting();

let perdag: dag.Store | undefined;
teardown(async () => {
  await perdag?.close();
});

async function deleteClientGroupForTesting<
  // eslint-disable-next-line @typescript-eslint/ban-types
  MD extends MutatorDefs = {},
>(rep: ReplicacheTest<MD>) {
  const clientGroupID = await rep.clientGroupID;
  assert(clientGroupID);
  await withWrite(rep.perdag, async tx => {
    await deleteClientGroup(clientGroupID, tx);
    await tx.commit();
  });
}

test('basic persist & load', async () => {
  const pullURL = 'https://diff.com/pull';
  const rep = await replicacheForTesting('persist-test', {
    pullURL,
  });
  const clientID = await rep.clientID;

  perdag = new dag.StoreImpl(
    new kv.IDBStore(rep.idbName),
    dag.uuidChunkHasher,
    assertHash,
  );

  const clientBeforePull = await withRead(perdag, read =>
    persist.getClient(clientID, read),
  );
  assertNotUndefined(clientBeforePull);

  assertClientDD31(clientBeforePull);
  const clientGroupBeforePull = await withRead(perdag, read =>
    persist.getClientGroup(clientBeforePull.clientGroupID, read),
  );
  assertNotUndefined(clientGroupBeforePull);

  fetchMock.postOnce(
    pullURL,
    makePullResponseDD31(clientID, 2, [
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

  rep.pull();

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
    await tickAFewTimes(waitMs);
    assertClientDD31(clientBeforePull);
    assertNotUndefined(clientGroupBeforePull);
    const clientGroup: persist.ClientGroup | undefined = await withRead(
      perdag,
      read => persist.getClientGroup(clientBeforePull.clientGroupID, read),
    );
    assertNotUndefined(clientGroup);
    if (clientGroupBeforePull.headHash !== clientGroup.headHash) {
      // persist has completed
      break;
    }
  }

  await rep.query(async tx => {
    expect(await tx.get('a')).to.equal(1);
    expect(await tx.get('b')).to.equal(2);
  });

  // If we create another instance it will lazy load the data from IDB
  const rep2 = await replicacheForTesting(
    rep.name,
    {
      pullURL,
    },
    {useUniqueName: false},
  );
  await rep2.query(async tx => {
    expect(await tx.get('a')).to.equal(1);
    expect(await tx.get('b')).to.equal(2);
  });

  expect(await rep.clientID).to.not.equal(await rep2.clientID);

  await perdag.close();
});

suite('onClientStateNotFound', () => {
  test('Called in persist if collected', async () => {
    const consoleErrorStub = sinon.stub(console, 'error');

    const rep = await replicacheForTesting('called-in-persist', {
      mutators: {addData},
    });

    await rep.mutate.addData({foo: 'bar'});
    await rep.persist();

    const clientID = await rep.clientID;
    await deleteClientForTesting(clientID, rep.perdag);

    const onClientStateNotFound = sinon.fake();
    rep.onClientStateNotFound = onClientStateNotFound;
    await rep.persist();

    expect(onClientStateNotFound.callCount).to.equal(1);
    expect(onClientStateNotFound.lastCall.args).to.deep.equal([]);
    expectLogContext(
      consoleErrorStub,
      0,
      rep,
      `Client state not found, clientID: ${clientID}`,
    );
  });

  test('Called in query if collected', async () => {
    const consoleErrorStub = sinon.stub(console, 'error');

    const name = 'called-in-query';
    const mutators = {
      addData,
    };
    const rep = await replicacheForTesting(name, {
      mutators,
      ...disableAllBackgroundProcesses,
    });

    await rep.mutate.addData({foo: 'bar'});
    await rep.persist();
    const clientID = await rep.clientID;
    await deleteClientForTesting(clientID, rep.perdag);

    // Need a real timeout here.
    clock.restore();
    await sleep(10);

    await rep.close();

    const rep2 = await replicacheForTesting(
      rep.name,
      {
        mutators,
        ...disableAllBackgroundProcesses,
        // To ensure query has to go to perdag, prevent pull from happening and
        // populating the lazy store cache.
        enablePullAndPushInOpen: false,
      },
      // Use same idb and client group as above rep.
      {useUniqueName: false},
    );

    const clientID2 = await rep2.clientID;

    await deleteClientForTesting(clientID2, rep2.perdag);

    // Cannot simply gcClientGroups because the client group has pending mutations.
    await deleteClientGroupForTesting(rep2);

    const onClientStateNotFound = sinon.fake();
    rep2.onClientStateNotFound = onClientStateNotFound;

    let e: unknown;
    try {
      await rep2.query(async tx => {
        await tx.get('foo');
      });
    } catch (err) {
      e = err;
    }
    expect(e).to.be.instanceOf(persist.ClientStateNotFoundError);
    expectLogContext(
      consoleErrorStub,
      0,
      rep2,
      `Client state not found, clientID: ${clientID2}`,
    );
    expect(onClientStateNotFound.lastCall.args).to.deep.equal([]);
  });

  test('Called in mutate if collected', async () => {
    const consoleErrorStub = sinon.stub(console, 'error');
    const name = 'called-in-mutate';
    const mutators = {
      addData,
      async check(tx: WriteTransaction, key: string) {
        await tx.has(key);
      },
    };

    const rep = await replicacheForTesting(name, {
      mutators,
      ...disableAllBackgroundProcesses,
    });

    await rep.mutate.addData({foo: 'bar'});
    await rep.persist();
    const clientID = await rep.clientID;
    await deleteClientForTesting(clientID, rep.perdag);
    await rep.close();

    const rep2 = await replicacheForTesting(
      rep.name,
      {
        mutators,
        ...disableAllBackgroundProcesses,
        // To ensure mutate has to go to perdag, prevent pull from happening and
        // populating the lazy store cache.
        enablePullAndPushInOpen: false,
      },
      // Use same idb and client group as above rep.
      {useUniqueName: false},
    );

    const clientID2 = await rep2.clientID;
    await deleteClientForTesting(clientID2, rep2.perdag);

    // Cannot simply gcClientGroups because the client group has pending mutations.
    await deleteClientGroupForTesting(rep2);

    const onClientStateNotFound = sinon.fake();
    rep2.onClientStateNotFound = onClientStateNotFound;

    let e: unknown;
    try {
      // Another mutate will trigger
      await rep2.mutate.check('x');
    } catch (err) {
      e = err;
    }

    expect(e).to.be.instanceOf(persist.ClientStateNotFoundError);
    expectLogContext(
      consoleErrorStub,
      0,
      rep2,
      `Client state not found, clientID: ${clientID2}`,
    );
    expect(onClientStateNotFound.lastCall.args).to.deep.equal([]);
  });
});

test('Persist throws if idb dropped', async () => {
  const rep = await replicacheForTesting(
    'called-in-persist-dropped',
    {
      mutators: {addData},
      ...disableAllBackgroundProcesses,
    },
    {useUniqueName: false},
  );

  await rep.mutate.addData({foo: 'bar'});

  await kv.dropIDBStore(rep.idbName);

  const onClientStateNotFound = sinon.fake();
  rep.onClientStateNotFound = onClientStateNotFound;
  let err;
  try {
    await rep.persist();
  } catch (e) {
    err = e;
  }
  expect(err).to.be.instanceOf(IDBNotFoundError);

  await rep.close();
});
