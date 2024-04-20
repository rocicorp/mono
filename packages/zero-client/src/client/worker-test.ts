// This test file is loaded by worker.test.ts

import {expect} from 'chai';
import {sleep} from 'shared/src/sleep.js';
import {zeroForTest} from './test-utils.js';

onmessage = async (e: MessageEvent) => {
  const {userID} = e.data;
  try {
    await testBasics(userID);
    postMessage(undefined);
  } catch (ex) {
    postMessage(ex);
  }
};

async function testBasics(userID: string) {
  console.log('testBasics', WebSocket);

  const r = zeroForTest({
    userID,
    mutators: {
      async inc(tx, key: string) {
        const v = (await tx.get<number>(key)) ?? 0;
        await tx.set(key, v + 1);
      },
    },
  });
  await r.triggerConnected();

  const log: (number | undefined)[] = [];
  const cancelSubscribe = r.subscribe(
    tx => tx.get<number>('foo'),
    v => log.push(v),
  );

  await sleep(1);
  expect(log).deep.equal([undefined]);

  await r.mutate.inc('foo');
  expect(log).deep.equal([undefined, 1]);
  expect(await r.query(tx => tx.get('foo'))).equal(1);

  await r.mutate.inc('foo');
  expect(log).deep.equal([undefined, 1, 2]);
  expect(await r.query(tx => tx.get('foo'))).equal(2);

  cancelSubscribe();

  await r.mutate.inc('foo');
  expect(log).deep.equal([undefined, 1, 2]);
  expect(await r.query(tx => tx.get('foo'))).equal(3);
}
