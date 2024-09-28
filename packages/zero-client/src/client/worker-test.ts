// This test file is loaded by worker.test.ts

import {assert} from 'shared/src/asserts.js';
import {deepEqual} from 'shared/src/json.js';
import {sleep} from 'shared/src/sleep.js';
import sinon from 'sinon';
import {MockSocket, zeroForTest} from './test-utils.js';
import {version} from './version.js';

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
  console.log('testBasics', WebSocket, version);

  type E = {
    id: string;
    value: number;
  };

  const r = zeroForTest({
    userID,
    schemas: {
      e: {
        tableName: 'e',
        columns: {
          id: {type: 'string'},
          value: {type: 'number'},
        },
        primaryKey: ['id'],
        relationships: {},
      },
    },
  });

  const q = r.query.e.select('id', 'value').limit(1);
  const view = q.materialize();
  view.hydrate();
  const log: (readonly E[])[] = [];
  const removeListener = view.addListener(rows => {
    log.push([...rows]);
  });

  await r.triggerConnected();

  await sleep(1);
  assert(deepEqual(log, [[]]));

  await r.mutate.e.set({id: 'foo', value: 1});
  assert(deepEqual(log, [[], [{id: 'foo', value: 1}]]));

  await r.mutate.e.set({id: 'foo', value: 2});
  assert(
    deepEqual(log, [[], [{id: 'foo', value: 1}], [{id: 'foo', value: 2}]]),
  );

  removeListener();

  await r.mutate.e.set({id: 'foo', value: 3});
  assert(
    deepEqual(log, [[], [{id: 'foo', value: 1}], [{id: 'foo', value: 2}]]),
  );

  const view2 = q.materialize();
  view2.hydrate();
  let data: E[] = [];
  view2.addListener(rows => {
    data = [...rows];
  });
  assert(deepEqual(data, [{id: 'foo', value: 3}]));
}
