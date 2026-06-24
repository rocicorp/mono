import type {HandoffResult} from './handoff.ts';
import {
  formatSeconds,
  messagesPerSubscriber,
  transactionsPerSecond,
} from './load-model.ts';

export function formatReport(results: readonly HandoffResult[]): string {
  const maxScenarioLength = Math.max(
    ...results.map(r => r.scenario.name.length),
  );
  const maxModeLength = Math.max(...results.map(r => r.mode.length));
  const lines = [
    'catchup backlog handoff benchmark',
    [
      'scenario'.padEnd(maxScenarioLength),
      'subs'.padStart(4),
      'tx/s'.padStart(6),
      'catchup'.padStart(7),
      'msgs/sub'.padStart(9),
      'payload'.padStart(8),
      'mode'.padEnd(maxModeLength),
      'report ms'.padStart(10),
      'actual ms'.padStart(10),
      'false ms'.padStart(10),
      'msg/s'.padStart(12),
      'pending@report'.padStart(14),
      'max agg pending'.padStart(15),
      'max/sub'.padStart(9),
    ].join(' | '),
    [
      ''.padEnd(maxScenarioLength, '-'),
      ''.padStart(4, '-'),
      ''.padStart(6, '-'),
      ''.padStart(7, '-'),
      ''.padStart(9, '-'),
      ''.padStart(8, '-'),
      ''.padEnd(maxModeLength, '-'),
      ''.padStart(10, '-'),
      ''.padStart(10, '-'),
      ''.padStart(10, '-'),
      ''.padStart(12, '-'),
      ''.padStart(14, '-'),
      ''.padStart(15, '-'),
      ''.padStart(9, '-'),
    ].join('-|-'),
  ];

  for (const result of results) {
    const {scenario} = result;
    const tps = transactionsPerSecond(scenario);
    lines.push(
      [
        scenario.name.padEnd(maxScenarioLength),
        scenario.subscribers.toLocaleString().padStart(4),
        (tps === undefined ? '-' : tps.toLocaleString()).padStart(6),
        formatSeconds(result.assumedCatchupMs).padStart(7),
        messagesPerSubscriber(scenario).toLocaleString().padStart(9),
        formatBytes(scenario.payloadBytes).padStart(8),
        result.mode.padEnd(maxModeLength),
        result.reportedCaughtUpMs.toFixed(1).padStart(10),
        result.actualCaughtUpMs.toFixed(1).padStart(10),
        result.hiddenDrainMs.toFixed(1).padStart(10),
        result.messagesPerSecond.toFixed(0).padStart(12),
        result.pendingAtProducerDone.toLocaleString().padStart(14),
        result.maxAggregatePending.toLocaleString().padStart(15),
        result.maxPendingPerSubscriber.toLocaleString().padStart(9),
      ].join(' | '),
    );
  }

  return lines.join('\n');
}

function formatBytes(bytes: number) {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  return `${Math.round(bytes / 1024)} KiB`;
}
