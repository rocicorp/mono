import {
  initReplicacheTesting,
  makePullResponse,
  replicacheForTesting,
  tickAFewTimes,
} from './test-util.js';
import type {WriteTransaction} from './mod.js';
import type {JSONValue} from './json.js';
import {expect} from '@esm-bundle/chai';
// fetch-mock has invalid d.ts file so we removed that on npm install.
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-expect-error
import fetchMock from 'fetch-mock/esm/client';

initReplicacheTesting();

async function addData(tx: WriteTransaction, data: {[key: string]: JSONValue}) {
  for (const [key, value] of Object.entries(data)) {
    await tx.put(key, value);
  }
}

test('pending mutation', async () => {
  const rep = await replicacheForTesting('pending-mutation', {
    mutators: {addData, del: (tx, key) => tx.del(key)},
  });

  const pendingMutations0 = await rep.experimentalPendingMutations();
  expect(pendingMutations0).to.deep.equal([]);

  await rep.mutate.addData({a: 1, b: 2});
  const addABMutation = {id: 1, name: 'addData', args: {a: 1, b: 2}};
  const pendingMutations1 = await rep.experimentalPendingMutations();
  expect(pendingMutations1).to.deep.equal([addABMutation]);

  const delBMutation = {id: 2, name: 'del', args: 'b'};
  await rep.mutate.del('b');
  const pendingMutations2 = await rep.experimentalPendingMutations();
  expect(pendingMutations2).to.deep.equal([delBMutation, addABMutation]);

  rep.pullURL = 'https://diff.com/pull';
  const clientID = await rep.clientID;
  fetchMock.post(rep.pullURL, makePullResponse(clientID, 2));
  rep.pull();
  await tickAFewTimes(100);
  await rep.mutate.addData({a: 3});
  const addAMutation = {id: 3, name: 'addData', args: {a: 3}};
  const pendingMutations3 = await rep.experimentalPendingMutations();
  expect(pendingMutations3).to.deep.equal([addAMutation]);

  fetchMock.reset();
  fetchMock.post(rep.pullURL, makePullResponse(clientID, 3));
  rep.pull();
  await tickAFewTimes(100);
  const pendingMutations4 = await rep.experimentalPendingMutations();
  expect(pendingMutations4).to.deep.equal([]);
});
