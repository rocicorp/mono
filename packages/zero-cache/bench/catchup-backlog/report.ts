import {MESSAGE_COUNT, type HandoffResult} from './handoff.ts';

export function formatReport(results: readonly HandoffResult[]): string {
  const maxModeLength = Math.max(...results.map(r => r.mode.length));
  const lines = [
    `catchup backlog handoff benchmark (${MESSAGE_COUNT.toLocaleString()} messages)`,
    [
      'mode'.padEnd(maxModeLength),
      'producer ms'.padStart(12),
      'total ms'.padStart(10),
      'msg/s'.padStart(12),
      'max pending'.padStart(12),
    ].join(' | '),
    [
      ''.padEnd(maxModeLength, '-'),
      ''.padStart(12, '-'),
      ''.padStart(10, '-'),
      ''.padStart(12, '-'),
      ''.padStart(12, '-'),
    ].join('-|-'),
  ];

  for (const result of results) {
    lines.push(
      [
        result.mode.padEnd(maxModeLength),
        result.producerMs.toFixed(1).padStart(12),
        result.totalMs.toFixed(1).padStart(10),
        result.messagesPerSecond.toFixed(0).padStart(12),
        result.maxDownstreamPending.toLocaleString().padStart(12),
      ].join(' | '),
    );
  }

  return lines.join('\n');
}
