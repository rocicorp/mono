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

initReplicacheTesting();

async function addData(tx: WriteTransaction, data: {[key: string]: JSONValue}) {
  for (const [key, value] of Object.entries(data)) {
    await tx.put(key, value);
  }
}

test('pending mutation', async () => {
  const rep = await replicacheForTesting('pending-mutation', {
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
    },
  });

  const clientID = await rep.clientID;

  expect(await rep.experimentalPendingMutations()).to.deep.equal([]);

  await rep.mutate.addData({a: 1, b: 2});
  const addABMutation = {id: 1, name: 'addData', args: {a: 1, b: 2}, clientID};
  expect(await rep.experimentalPendingMutations()).to.deep.equal([
    addABMutation,
  ]);

  const delBMutation = {id: 2, name: 'del', args: 'b', clientID};
  await rep.mutate.del('b');
  expect(await rep.experimentalPendingMutations()).to.deep.equal([
    delBMutation,
    addABMutation,
  ]);

  rep.pullURL = 'https://diff.com/pull';
  fetchMock.post(rep.pullURL, makePullResponseV1(clientID, 2));
  rep.pull();
  await tickAFewTimes(100);
  await rep.mutate.addData({a: 3});
  const addAMutation = {id: 3, name: 'addData', args: {a: 3}, clientID};
  expect(await rep.experimentalPendingMutations()).to.deep.equal([
    addAMutation,
  ]);

  fetchMock.reset();
  fetchMock.post(rep.pullURL, makePullResponseV1(clientID, 3));
  rep.pull();
  await tickAFewTimes(100);
  expect(await rep.experimentalPendingMutations()).to.deep.equal([]);
});
