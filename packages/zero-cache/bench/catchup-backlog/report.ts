import type {HandoffResult} from './handoff.ts';

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
      'msgs/sub'.padStart(9),
      'payload'.padStart(8),
      'mode'.padEnd(maxModeLength),
      'done ms'.padStart(9),
      'total ms'.padStart(10),
      'hidden ms'.padStart(10),
      'msg/s'.padStart(12),
      'pending at done'.padStart(15),
      'max agg pending'.padStart(15),
      'max/sub'.padStart(9),
    ].join(' | '),
    [
      ''.padEnd(maxScenarioLength, '-'),
      ''.padStart(4, '-'),
      ''.padStart(9, '-'),
      ''.padStart(8, '-'),
      ''.padEnd(maxModeLength, '-'),
      ''.padStart(9, '-'),
      ''.padStart(10, '-'),
      ''.padStart(10, '-'),
      ''.padStart(12, '-'),
      ''.padStart(15, '-'),
      ''.padStart(15, '-'),
      ''.padStart(9, '-'),
    ].join('-|-'),
  ];

  for (const result of results) {
    const {scenario} = result;
    lines.push(
      [
        scenario.name.padEnd(maxScenarioLength),
        scenario.subscribers.toLocaleString().padStart(4),
        scenario.messagesPerSubscriber.toLocaleString().padStart(9),
        formatBytes(scenario.payloadBytes).padStart(8),
        result.mode.padEnd(maxModeLength),
        result.producerMs.toFixed(1).padStart(9),
        result.totalMs.toFixed(1).padStart(10),
        result.hiddenDrainMs.toFixed(1).padStart(10),
        result.messagesPerSecond.toFixed(0).padStart(12),
        result.pendingAtProducerDone.toLocaleString().padStart(15),
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
