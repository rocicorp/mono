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

globalThis.gc();
const initialHeap = process.memoryUsage().heapUsed;

const backlogs = Array.from({length: scenario.subscribers}, (_, subscriber) =>
  Array.from({length: scenario.messagesPerSubscriber}, (_, message) =>
    createPayload(subscriber, message, scenario.payloadBytes),
  ),
);
globalThis.gc();
const backlogHeap = process.memoryUsage().heapUsed;

const oldDownstreams = Array.from({length: scenario.subscribers}, () =>
  Subscription.create<string>(),
);
for (let subscriber = 0; subscriber < scenario.subscribers; subscriber++) {
  const downstream = oldDownstreams[subscriber];
  for (const payload of backlogs[subscriber]) {
    downstream.push(payload);
  }
}
globalThis.gc();
const oldHandoffHeap = process.memoryUsage().heapUsed;

const newDownstreams = Array.from({length: scenario.subscribers}, () =>
  Subscription.create<string>(),
);
for (let subscriber = 0; subscriber < scenario.subscribers; subscriber++) {
  newDownstreams[subscriber].push(backlogs[subscriber][0]);
}
globalThis.gc();
const newHandoffHeap = process.memoryUsage().heapUsed;

const backlogEntries = scenario.subscribers * scenario.messagesPerSubscriber;
const oldQueued = countPending(oldDownstreams);
const newQueued = countPending(newDownstreams);
const backlogDelta = backlogHeap - initialHeap;
const oldExtraDelta = oldHandoffHeap - backlogHeap;
const newExtraDelta = newHandoffHeap - oldHandoffHeap;

console.log(
  [
    `scenario: ${scenario.name}`,
    `backlog entries: ${backlogEntries.toLocaleString()}`,
    `private backlog heap: ${formatMB(backlogDelta)}`,
    `old handoff queued entries: ${oldQueued.toLocaleString()}`,
    `old handoff extra heap: ${formatMB(oldExtraDelta)}`,
    `new handoff queued entries: ${newQueued.toLocaleString()}`,
    `new handoff extra heap: ${formatMB(newExtraDelta)}`,
    `old extra bytes / queued entry: ${(oldExtraDelta / oldQueued).toFixed(0)}`,
  ].join('\n'),
);

function countPending(downstreams: Subscription<string>[]) {
  return downstreams.reduce(
    (total, downstream) => total + downstream.queued + downstream.consuming,
    0,
  );
}

function formatMB(bytes: number) {
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}
