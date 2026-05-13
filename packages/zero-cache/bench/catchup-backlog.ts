/* oxlint-disable no-console */
import {setImmediate as yieldImmediate} from 'node:timers/promises';
import {Subscription} from '../src/types/subscription.ts';

const MESSAGE_COUNT = 100_000;
const YIELD_EVERY = 512;
const PAYLOAD = JSON.stringify([
  'data',
  {
    tag: 'insert',
    relation: {schema: 'public', name: 'issues', replicaIdentity: 'default'},
    new: {id: 'issue-1', title: 'the view-syncer is behind', owner: 'rm'},
  },
]);

// Reproduces the catchup handoff risk fixed by the subscriber backlog change.
//
//   storer catchup cursor
//          |
//          v
//   subscriber backlog  ---> downstream websocket
//
// The unsafe shape is a fire-and-forget handoff: the storer can finish loading
// catchup rows while the downstream queue is still holding a huge number of
// unacked messages. The golden path is flow-controlled handoff: every backlog
// entry resolves only after the downstream push is consumed.
type Mode = 'fire-and-forget handoff' | 'flow-controlled handoff';

type Result = {
  mode: Mode;
  producerMs: number;
  totalMs: number;
  messagesPerSecond: number;
  maxDownstreamPending: number;
};

async function run(mode: Mode): Promise<Result> {
  const downstream = Subscription.create<string>();
  let consumed = 0;
  let maxDownstreamPending = 0;

  const samplePending = () => {
    maxDownstreamPending = Math.max(
      maxDownstreamPending,
      downstream.queued + downstream.consuming,
    );
  };

  const consumer = (async () => {
    const iter = downstream[Symbol.asyncIterator]();
    while (consumed < MESSAGE_COUNT) {
      const next = await iter.next();
      if (next.done) {
        throw new Error('downstream ended before consuming all messages');
      }
      JSON.parse(next.value);
      consumed++;
      samplePending();
      if (consumed % YIELD_EVERY === 0) {
        await yieldImmediate();
      }
    }
    await iter.return?.();
  })();

  const start = performance.now();
  let producerMs: number;
  if (mode === 'fire-and-forget handoff') {
    const pending: Promise<unknown>[] = [];
    for (let i = 0; i < MESSAGE_COUNT; i++) {
      pending.push(downstream.push(PAYLOAD).result);
      if (i % YIELD_EVERY === 0) {
        samplePending();
      }
    }
    producerMs = performance.now() - start;
    await Promise.all(pending);
  } else {
    for (let i = 0; i < MESSAGE_COUNT; i++) {
      const {result} = downstream.push(PAYLOAD);
      samplePending();
      await result;
    }
    producerMs = performance.now() - start;
  }
  await consumer;
  const totalMs = performance.now() - start;

  return {
    mode,
    producerMs,
    totalMs,
    messagesPerSecond: MESSAGE_COUNT / (totalMs / 1000),
    maxDownstreamPending,
  };
}

const results = [
  await run('fire-and-forget handoff'),
  await run('flow-controlled handoff'),
];

const maxModeLength = Math.max(...results.map(r => r.mode.length));
console.log(
  `catchup backlog handoff benchmark (${MESSAGE_COUNT.toLocaleString()} messages)`,
);
console.log(
  [
    'mode'.padEnd(maxModeLength),
    'producer ms'.padStart(12),
    'total ms'.padStart(10),
    'msg/s'.padStart(12),
    'max pending'.padStart(12),
  ].join(' | '),
);
console.log(
  [
    ''.padEnd(maxModeLength, '-'),
    ''.padStart(12, '-'),
    ''.padStart(10, '-'),
    ''.padStart(12, '-'),
    ''.padStart(12, '-'),
  ].join('-|-'),
);
for (const result of results) {
  console.log(
    [
      result.mode.padEnd(maxModeLength),
      result.producerMs.toFixed(1).padStart(12),
      result.totalMs.toFixed(1).padStart(10),
      result.messagesPerSecond.toFixed(0).padStart(12),
      result.maxDownstreamPending.toLocaleString().padStart(12),
    ].join(' | '),
  );
}
