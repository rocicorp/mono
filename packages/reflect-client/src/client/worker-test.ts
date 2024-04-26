// This test file is loaded by worker.test.ts

import {assert} from 'shared/src/asserts.js';
import {JSONValue, deepEqual} from 'shared/src/json.js';
import {sleep} from 'shared/src/sleep.js';
import {reflectForTest} from './test-utils.js';

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

  const r = reflectForTest({
    userID,
    mutators: {
      async inc(tx, key: string) {
        const v = (await tx.get<number>(key)) ?? 0;
        await tx.set(key, v + 1);
      },
    },
  });
  await r.triggerConnected();

  const log: JSONValue[] = [];
  const cancelSubscribe = r.subscribe(
    tx => tx.get<number>('foo'),
    v => log.push(v ?? '<NOT FOUND>'),
  );

  function assertLog(expected: JSONValue) {
    assert(deepEqual(log, expected));
  }

  await sleep(1);
  assertLog(['<NOT FOUND>']);

  await r.mutate.inc('foo');
  assertLog(['<NOT FOUND>', 1]);
  assert((await r.query(tx => tx.get('foo'))) === 1);

  await r.mutate.inc('foo');
  assertLog(['<NOT FOUND>', 1, 2]);
  assert((await r.query(tx => tx.get('foo'))) === 2);

  cancelSubscribe();

  await r.mutate.inc('foo');
  assertLog(['<NOT FOUND>', 1, 2]);
  assert((await r.query(tx => tx.get('foo'))) === 3);
}
