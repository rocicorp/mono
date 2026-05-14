import {
  setImmediate as yieldImmediate,
  setTimeout as delay,
} from 'node:timers/promises';
import {Subscription} from '../../src/types/subscription.ts';
import {createPayload} from './payload.ts';
import {scenarios, type HandoffScenario} from './scenarios.ts';

export type HandoffMode = 'fire-and-forget handoff' | 'flow-controlled handoff';

export type HandoffResult = {
  scenario: HandoffScenario;
  mode: HandoffMode;
  producerMs: number;
  totalMs: number;
  messagesPerSecond: number;
  maxAggregatePending: number;
  maxPendingPerSubscriber: number;
};

export async function runHandoffBenchmark(): Promise<HandoffResult[]> {
  const results: HandoffResult[] = [];
  for (const scenario of scenarios) {
    results.push(await run(scenario, 'fire-and-forget handoff'));
    results.push(await run(scenario, 'flow-controlled handoff'));
  }
  return results;
}

// #5970: https://github.com/rocicorp/mono/pull/5970
// This benchmark keeps the catchup handoff regression reproducible. The
// previous fire-and-forget handoff could report producer completion while a
// large catchup backlog remained queued downstream; the fixed path reports
// completion only after downstream consumption, keeping pending work bounded.
//
//   storer catchup cursor
//          |
//          v
//   subscriber backlog  ---> downstream websocket
async function run(
  scenario: HandoffScenario,
  mode: HandoffMode,
): Promise<HandoffResult> {
  const downstreams = Array.from({length: scenario.subscribers}, () =>
    Subscription.create<string>(),
  );
  const consumed = Array.from({length: scenario.subscribers}, () => 0);
  let maxAggregatePending = 0;
  let maxPendingPerSubscriber = 0;

  const samplePending = () => {
    let aggregate = 0;
    for (const downstream of downstreams) {
      const pending = downstream.queued + downstream.consuming;
      aggregate += pending;
      maxPendingPerSubscriber = Math.max(maxPendingPerSubscriber, pending);
    }
    maxAggregatePending = Math.max(maxAggregatePending, aggregate);
  };

  const consumers = downstreams.map(async (downstream, subscriber) => {
    const iter = downstream[Symbol.asyncIterator]();
    while (consumed[subscriber] < scenario.messagesPerSubscriber) {
      const next = await iter.next();
      if (next.done) {
        throw new Error(
          `downstream ${subscriber} ended before consuming all messages`,
        );
      }
      JSON.parse(next.value);
      consumed[subscriber]++;
      samplePending();
      if (consumed[subscriber] % scenario.yieldEvery === 0) {
        await yieldImmediate();
      }
      if (
        scenario.delayEvery &&
        scenario.delayMs &&
        consumed[subscriber] % scenario.delayEvery === 0
      ) {
        await delay(scenario.delayMs);
      }
    }
    await iter.return?.();
  });

  const pushPayload = (
    downstream: Subscription<string>,
    subscriber: number,
    message: number,
  ) =>
    downstream.push(createPayload(subscriber, message, scenario.payloadBytes))
      .result;

  const start = performance.now();
  let producerMs: number;
  if (mode === 'fire-and-forget handoff') {
    const pending: Promise<unknown>[] = [];
    for (let subscriber = 0; subscriber < downstreams.length; subscriber++) {
      const downstream = downstreams[subscriber];
      for (
        let message = 0;
        message < scenario.messagesPerSubscriber;
        message++
      ) {
        pending.push(pushPayload(downstream, subscriber, message));
        if (message % scenario.yieldEvery === 0) {
          samplePending();
        }
      }
    }
    producerMs = performance.now() - start;
    await Promise.all(pending);
  } else {
    await Promise.all(
      downstreams.map(async (downstream, subscriber) => {
        for (
          let message = 0;
          message < scenario.messagesPerSubscriber;
          message++
        ) {
          const result = pushPayload(downstream, subscriber, message);
          samplePending();
          await result;
        }
      }),
    );
    producerMs = performance.now() - start;
  }
  await Promise.all(consumers);
  const totalMs = performance.now() - start;

  const totalMessages = scenario.subscribers * scenario.messagesPerSubscriber;

  return {
    scenario,
    mode,
    producerMs,
    totalMs,
    messagesPerSecond: totalMessages / (totalMs / 1000),
    maxAggregatePending,
    maxPendingPerSubscriber,
  };
}
