// This test file is loaded by worker.test.ts

import {assert} from '../../../shared/src/asserts.ts';
import {deepEqual} from '../../../shared/src/json.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {defineMutatorsWithType} from '../../../zql/src/mutate/mutator-registry.ts';
import {defineMutatorWithType} from '../../../zql/src/mutate/mutator.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {MockSocket, zeroForTest} from './test-utils.ts';

const {WebSocket} = globalThis;

onmessage = async (e: MessageEvent) => {
  const {userID} = e.data;
  try {
    globalThis.WebSocket = MockSocket as unknown as typeof WebSocket;
    await testBasics(userID);
    postMessage(undefined);
  } catch (ex) {
    postMessage(ex);
  } finally {
    globalThis.WebSocket = WebSocket;
  }
};

// Tell the main thread that we're ready to receive messages.
postMessage('ready');

async function testBasics(userID: string) {
  type E = {
    id: string;
    value: number;
  };

  const schema = createSchema({
    tables: [
      table('e')
        .columns({
          id: string(),
          value: number(),
        })
        .primaryKey('id'),
    ],
  });
  const mutators = defineMutatorsWithType<typeof schema>()({
    upsertE: defineMutatorWithType<typeof schema>()<E>(({tx, args}) =>
      tx.mutate.e.upsert(args),
    ),
  });
  const {upsertE} = mutators;
  const z = zeroForTest({
    userID,
    schema,
    mutators,
  });
  const zql = createBuilder(schema);
  const q = zql.e.limit(1);
  const view = z.materialize(q);
  const log: (readonly E[])[] = [];
  const removeListener = view.addListener(rows => {
    // the array view nodes are edited in place, so we need to clone them
    // https://github.com/rocicorp/mono/pull/4576
    log.push(rows.map(row => ({...row})));
  });

  await z.triggerConnected();

  await sleep(1);
  assert(deepEqual(log, [[]]));

  await z.mutate(upsertE({id: 'foo', value: 1})).client;
  assert(deepEqual(log, [[], [{id: 'foo', value: 1}]]));

  await z.mutate(upsertE({id: 'foo', value: 2})).client;
  assert(
    deepEqual(log, [[], [{id: 'foo', value: 1}], [{id: 'foo', value: 2}]]),
  );

  removeListener();

  await z.mutate(upsertE({id: 'foo', value: 3})).client;
  assert(
    deepEqual(log, [[], [{id: 'foo', value: 1}], [{id: 'foo', value: 2}]]),
  );

  const view2 = z.materialize(q);
  let data: E[] = [];
  view2.addListener(rows => {
    data = [...rows];
  });
  assert(deepEqual(data, [{id: 'foo', value: 3}]));
}
