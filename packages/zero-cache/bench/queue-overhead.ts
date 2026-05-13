/* oxlint-disable no-console */
import {performance} from 'node:perf_hooks';
import {Queue} from '../../shared/src/queue.ts';
import {Subscription} from '../src/types/subscription.ts';

type Result = {
  readonly scenario: string;
  readonly mode: string;
  readonly count: number;
  readonly elapsedMs: number;
  readonly opsPerSec: number;
};

type Summary = {
  readonly name: 'queue-overhead';
  readonly generatedAt: string;
  readonly count: number;
  readonly results: Result[];
};

const count = envInt('ZERO_QUEUE_BENCH_COUNT', 200_000);
const results: Result[] = [];

results.push(runQueueShiftBaseline(count));
results.push(runQueue(count));
results.push(runSubscriptionShiftBaseline(count));
results.push(await runSubscription(count));

for (const result of results) {
  console.log(
    `${result.scenario} ${result.mode}: ` +
      `${formatRate(result.opsPerSec)} ops/s | ${result.elapsedMs.toFixed(1)} ms`,
  );
}

const summary: Summary = {
  name: 'queue-overhead',
  generatedAt: new Date().toISOString(),
  count,
  results,
};
console.log(JSON.stringify(summary));

function runQueueShiftBaseline(count: number): Result {
  const values: number[] = [];
  const start = performance.now();
  for (let i = 0; i < count; i++) {
    values.push(i);
  }
  for (let i = 0; i < count; i++) {
    values.shift();
  }
  return result('Queue FIFO', 'array-shift baseline', count, start);
}

function runQueue(count: number): Result {
  const queue = new Queue<number>();
  const start = performance.now();
  for (let i = 0; i < count; i++) {
    queue.enqueue(i);
  }
  for (let i = 0; i < count; i++) {
    void queue.dequeue();
  }
  return result('Queue FIFO', 'head-index deque', count, start);
}

function runSubscriptionShiftBaseline(count: number): Result {
  const values: number[] = [];
  const start = performance.now();
  for (let i = 0; i < count; i++) {
    values.push(i);
  }
  for (let i = 0; i < count; i++) {
    values.shift();
  }
  return result(
    'Subscription queued drain',
    'array-shift baseline',
    count,
    start,
  );
}

async function runSubscription(count: number): Promise<Result> {
  const subscription = Subscription.create<number>();
  for (let i = 0; i < count; i++) {
    subscription.push(i);
  }
  subscription.end();

  const start = performance.now();
  for await (const _ of subscription) {
    // Drain.
  }
  return result('Subscription queued drain', 'head-index deque', count, start);
}

function result(
  scenario: string,
  mode: string,
  count: number,
  start: number,
): Result {
  const elapsedMs = performance.now() - start;
  return {
    scenario,
    mode,
    count,
    elapsedMs,
    opsPerSec: count / (elapsedMs / 1000),
  };
}

function envInt(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined || value === '') {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid integer ${name}=${value}`);
  }
  return parsed;
}

function formatRate(value: number): string {
  return value.toLocaleString('en-US', {maximumFractionDigits: 1});
}
