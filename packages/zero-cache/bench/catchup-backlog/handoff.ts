import {setImmediate as yieldImmediate} from 'node:timers/promises';
import {Subscription} from '../../src/types/subscription.ts';

export const MESSAGE_COUNT = 100_000;
const YIELD_EVERY = 512;
const PAYLOAD = JSON.stringify([
  'data',
  {
    tag: 'insert',
    relation: {schema: 'public', name: 'issues', replicaIdentity: 'default'},
    new: {id: 'issue-1', title: 'the view-syncer is behind', owner: 'rm'},
  },
]);

export type HandoffMode = 'fire-and-forget handoff' | 'flow-controlled handoff';

export type HandoffResult = {
  mode: HandoffMode;
  producerMs: number;
  totalMs: number;
  messagesPerSecond: number;
  maxDownstreamPending: number;
};

export async function runHandoffBenchmark(): Promise<HandoffResult[]> {
  return [
    await run('fire-and-forget handoff'),
    await run('flow-controlled handoff'),
  ];
}

// #5970: https://github.com/rocicorp/mono/pull/5970
// This benchmark keeps the catchup handoff regression reproducible. The
// previous fire-and-forget handoff could report producer completion while
// leaving 100k messages queued downstream; the fixed path reports completion
// only after downstream consumption, keeping max pending at 1 in this harness.
//
//   storer catchup cursor
//          |
//          v
//   subscriber backlog  ---> downstream websocket
async function run(mode: HandoffMode): Promise<HandoffResult> {
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
