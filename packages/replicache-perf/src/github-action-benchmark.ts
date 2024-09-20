import type {BenchmarkResult} from './benchmark.js';
import {formatAsReplicache, formatVariance} from './format.js';

// See https://github.com/benchmark-action/github-action-benchmark#examples
export type Entry = {
  name: string;
  unit: string;
  value: number;
  // variance
  range?: string;
  // any extra info, will be displayed in tool tip on graphs
  extra?: string;
};

export function createGithubActionBenchmarkJSONEntries(
  result: BenchmarkResult,
): Entry[] {
  return [
    {
      name: result.name,
      unit: 'median ms',
      value: result.runTimesStatistics.medianMs,
      range: formatVariance(result.runTimesStatistics.variance),
      extra: formatAsReplicache(result),
    },
    {
      name: `${result.name} p95`,
      unit: 'p95 ms',
      value: result.runTimesStatistics.p95Ms,
      range: formatVariance(result.runTimesStatistics.variance),
      extra: formatAsReplicache(result),
    },
  ];
}
