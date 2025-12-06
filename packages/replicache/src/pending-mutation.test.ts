import {expect, test, vi} from 'vitest';
import type {JSONValue} from '../../shared/src/json.ts';
import {TestStore} from './dag/test-store.ts';
import {ChainBuilder} from './db/test-helpers.ts';
import {pendingMutationsForAPI} from './pending-mutations.ts';
import {
  fetchMocker,
  initReplicacheTesting,
  makePullResponseV1,
  replicacheForTesting,
  tickAFewTimes,
} from './test-util.ts';
import type {WriteTransaction} from './transactions.ts';
import {withRead} from './with-transactions.ts';

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

  expect(await rep.experimentalPendingMutations()).toEqual([]);

  await rep.mutate.addData({a: 1, b: 2});
  const addABMutation = {id: 1, name: 'addData', args: {a: 1, b: 2}, clientID};
  expect(await rep.experimentalPendingMutations()).toEqual([addABMutation]);

  const delBMutation = {id: 2, name: 'del', args: 'b', clientID};
  await rep.mutate.del('b');
  expect(await rep.experimentalPendingMutations()).toEqual([
    addABMutation,
    delBMutation,
  ]);

  rep.pullURL = 'https://diff.com/pull';
  fetchMocker.post(rep.pullURL, makePullResponseV1(clientID, 2, undefined, 1));
  rep.pullIgnorePromise();
  await tickAFewTimes(vi, 100);
  await rep.mutate.addData({a: 3});
  const addAMutation = {id: 3, name: 'addData', args: {a: 3}, clientID};
  expect(await rep.experimentalPendingMutations()).toEqual([addAMutation]);

  fetchMocker.reset();
  fetchMocker.post(rep.pullURL, makePullResponseV1(clientID, 3, undefined, 2));
  rep.pullIgnorePromise();
  await tickAFewTimes(vi, 100);
  expect(await rep.experimentalPendingMutations()).toEqual([]);
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
    expect(await pendingMutationsForAPI(dagRead)).toEqual([
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
    expect(await pendingMutationsForAPI(dagRead)).toEqual([
      {id: 11, name: 'mutator_name_2', args: [2], clientID: clientID1},
      {id: 21, name: 'mutator_name_3', args: [3], clientID: clientID2},
      {id: 12, name: 'mutator_name_4', args: [4], clientID: clientID1},
      {id: 22, name: 'mutator_name_5', args: [5], clientID: clientID2},
    ]);
  });
});
