// This test file is loaded by worker.test.ts

import sinon from 'sinon';
import {assert} from '../../../shared/src/asserts.ts';
import {deepEqual} from '../../../shared/src/json.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {MockSocket, zeroForTest} from './test-utils.ts';

onmessage = async (e: MessageEvent) => {
  const {userID} = e.data;
  try {
    sinon.replace(
      globalThis,
      'WebSocket',
      MockSocket as unknown as typeof WebSocket,
    );
    await testBasics(userID);
    postMessage(undefined);
  } catch (ex) {
    postMessage(ex);
  } finally {
    sinon.restore();
  }
};

// Tell the main thread that we're ready to receive messages.
postMessage('ready');

async function testBasics(userID: string) {
  type E = {
    id: string;
    value: number;
  };

  const r = zeroForTest({
    userID,
    schema: createSchema(1, {
      tables: [
        table('e')
          .columns({
            id: string(),
            value: number(),
          })
          .primaryKey('id'),
      ],
    }),
  });

  const q = r.query.e.limit(1);
  const view = q.materialize();
  const log: (readonly E[])[] = [];
  const removeListener = view.addListener(rows => {
    log.push([...rows]);
  });

  await r.triggerConnected();

  await sleep(1);
  assert(deepEqual(log, [[]]));

  await r.mutate.e.upsert({id: 'foo', value: 1});
  assert(deepEqual(log, [[], [{id: 'foo', value: 1}]]));

  await r.mutate.e.upsert({id: 'foo', value: 2});
  assert(
    deepEqual(log, [[], [{id: 'foo', value: 1}], [{id: 'foo', value: 2}]]),
  );

  removeListener();

  await r.mutate.e.upsert({id: 'foo', value: 3});
  assert(
    deepEqual(log, [[], [{id: 'foo', value: 1}], [{id: 'foo', value: 2}]]),
  );

  const view2 = q.materialize();
  let data: E[] = [];
  view2.addListener(rows => {
    data = [...rows];
  });
  assert(deepEqual(data, [{id: 'foo', value: 3}]));
}
