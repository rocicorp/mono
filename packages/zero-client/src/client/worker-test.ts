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

  type E = {
    id: string;
    value: number;
  };

  const r = zeroForTest({
    userID,
    mutators: {
      async inc(tx, id: string) {
        const rows = await q.exec();
        const value = rows[0]?.value ?? 0;
        await tx.set(`e/${id}`, {id, value: value + 1});
      },
    },
    queries: {
      e: v => v as E,
    },
  });

  const q = r.query.e.select('*').limit(1).prepare();

  await r.triggerConnected();

  const log: (readonly E[])[] = [];
  const cancelSubscribe = q.subscribe(rows => {
    log.push(rows);
  });

  await sleep(1);
  expect(log).deep.equal([[]]);

  await r.mutate.inc('foo');
  expect(log).deep.equal([[], [{id: 'foo', value: 1}]]);

  await r.mutate.inc('foo');
  expect(log).deep.equal([
    [],
    [{id: 'foo', value: 1}],
    [{id: 'foo', value: 2}],
  ]);

  cancelSubscribe();

  await r.mutate.inc('foo');
  expect(log).deep.equal([
    [],
    [{id: 'foo', value: 1}],
    [{id: 'foo', value: 2}],
  ]);
  expect(await q.exec()).deep.equal([{id: 'foo', value: 3}]);
}
