import {expect} from 'chai';
import type {JSONValue} from './json.js';
import {
  initReplicacheTesting,
  makePullResponseV1,
  replicacheForTesting,
  tickAFewTimes,
} from './test-util.js';
import type {WriteTransaction} from './transactions.js';
// fetch-mock has invalid d.ts file so we removed that on npm install.
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-expect-error
import fetchMock from 'fetch-mock/esm/client';
import {TestStore} from './dag/test-store.js';
import {ChainBuilder} from './db/test-helpers.js';
import {pendingMutationsForAPI} from './pending-mutations.js';
import {withRead} from './with-transactions.js';

initReplicacheTesting();

async function addData(tx: WriteTransaction, data: {[key: string]: JSONValue}) {
  for (const [key, value] of Object.entries(data)) {
    await tx.set(key, value);
  }
}

test('pending mutation', async () => {
  const rep = await replicacheForTesting('pending-mutation', {
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
    },
  });

  const {clientID} = rep;

  expect(await rep.experimentalPendingMutations()).to.deep.equal([]);

  await rep.mutate.addData({a: 1, b: 2});
  const addABMutation = {id: 1, name: 'addData', args: {a: 1, b: 2}, clientID};
  expect(await rep.experimentalPendingMutations()).to.deep.equal([
    addABMutation,
  ]);

  const delBMutation = {id: 2, name: 'del', args: 'b', clientID};
  await rep.mutate.del('b');
  expect(await rep.experimentalPendingMutations()).to.deep.equal([
    addABMutation,
    delBMutation,
  ]);

  rep.pullURL = 'https://diff.com/pull';
  fetchMock.post(rep.pullURL, makePullResponseV1(clientID, 2));
  void rep.pull();
  await tickAFewTimes(100);
  await rep.mutate.addData({a: 3});
  const addAMutation = {id: 3, name: 'addData', args: {a: 3}, clientID};
  expect(await rep.experimentalPendingMutations()).to.deep.equal([
    addAMutation,
  ]);

  fetchMock.reset();
  fetchMock.post(rep.pullURL, makePullResponseV1(clientID, 3));
  void rep.pull();
  await tickAFewTimes(100);
  expect(await rep.experimentalPendingMutations()).to.deep.equal([]);
});

test('Test at a lower level', async () => {
  const clientID = 'client1';
  const store = new TestStore();
  const b = new ChainBuilder(store);
  await b.addGenesis(clientID);
  await b.addSnapshot([], clientID, 1, {
    [clientID]: 10,
  });
  await b.addLocal(clientID);
  await b.addLocal(clientID);
  await b.addLocal(clientID);
  await b.addLocal(clientID);

  await withRead(store, async dagRead => {
    expect(await pendingMutationsForAPI(dagRead)).to.deep.equal([
      {id: 11, name: 'mutator_name_2', args: [2], clientID},
      {id: 12, name: 'mutator_name_3', args: [3], clientID},
      {id: 13, name: 'mutator_name_4', args: [4], clientID},
      {id: 14, name: 'mutator_name_5', args: [5], clientID},
    ]);
  });
});

test('multiple clients', async () => {
  const clientID1 = 'client1';
  const clientID2 = 'client2';
  const store = new TestStore();
  const b = new ChainBuilder(store);
  await b.addGenesis(clientID1);
  await b.addSnapshot([], clientID1, 1, {
    [clientID1]: 10,
    [clientID2]: 20,
  });
  await b.addLocal(clientID1);
  await b.addLocal(clientID2);
  await b.addLocal(clientID1);
  await b.addLocal(clientID2);

  await withRead(store, async dagRead => {
    expect(await pendingMutationsForAPI(dagRead)).to.deep.equal([
      {id: 11, name: 'mutator_name_2', args: [2], clientID: clientID1},
      {id: 21, name: 'mutator_name_3', args: [3], clientID: clientID2},
      {id: 12, name: 'mutator_name_4', args: [4], clientID: clientID1},
      {id: 22, name: 'mutator_name_5', args: [5], clientID: clientID2},
    ]);
  });
});
