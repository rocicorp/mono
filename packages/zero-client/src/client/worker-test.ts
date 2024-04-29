// This test file is loaded by worker.test.ts

import {assert} from 'shared/src/asserts.js';
import {deepEqual} from 'shared/src/json.js';
import {sleep} from 'shared/src/sleep.js';
import {ENTITIES_KEY_PREFIX} from './keys.js';
import {zeroForTest} from './test-utils.js';
import {version} from './version.js';

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
  console.log('testBasics', WebSocket, version);

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
        await tx.set(`${ENTITIES_KEY_PREFIX}e/${id}`, {id, value: value + 1});
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
  assert(deepEqual(log, [[]]));

  await r.mutate.inc('foo');
  assert(deepEqual(log, [[], [{id: 'foo', value: 1}]]));

  await r.mutate.inc('foo');
  assert(
    deepEqual(log, [[], [{id: 'foo', value: 1}], [{id: 'foo', value: 2}]]),
  );

  cancelSubscribe();

  await r.mutate.inc('foo');
  assert(
    deepEqual(log, [[], [{id: 'foo', value: 1}], [{id: 'foo', value: 2}]]),
  );
  assert(deepEqual(await q.exec(), [{id: 'foo', value: 3}]));
}
