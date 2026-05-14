/* oxlint-disable no-console */
import {Subscription} from '../../src/types/subscription.ts';
import {createPayload} from './payload.ts';
import {scenarios} from './scenarios.ts';

const scenario = scenarios.find(s => s.name === '16-vs-outage-load');
if (!scenario) {
  throw new Error('missing 16-vs-outage-load scenario');
}

if (!globalThis.gc) {
  throw new Error(
    'run with: node --expose-gc --import tsx ./bench/catchup-backlog/memory.ts',
  );
}

const downstreams = Array.from({length: scenario.subscribers}, () =>
  Subscription.create<string>(),
);

globalThis.gc();
const before = process.memoryUsage().heapUsed;

for (let subscriber = 0; subscriber < scenario.subscribers; subscriber++) {
  const downstream = downstreams[subscriber];
  for (let message = 0; message < scenario.messagesPerSubscriber; message++) {
    downstream.push(createPayload(subscriber, message, scenario.payloadBytes));
  }
}

globalThis.gc();
const after = process.memoryUsage().heapUsed;
const queued = downstreams.reduce(
  (total, downstream) => total + downstream.queued + downstream.consuming,
  0,
);
const heapDelta = after - before;

console.log(
  [
    `scenario: ${scenario.name}`,
    `queued entries: ${queued.toLocaleString()}`,
    `heap delta: ${formatMB(heapDelta)}`,
    `bytes / queued entry: ${(heapDelta / queued).toFixed(0)}`,
  ].join('\n'),
);

function formatMB(bytes: number) {
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}
