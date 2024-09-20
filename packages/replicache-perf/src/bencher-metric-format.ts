import type {BenchmarkResult} from './benchmark.js';

export type BencherMetricsFormat = {
  [k: string]: {
    [k: string]: {
      value: number;
      ['lower_value']?: number;
      ['upper_value']?: number;
    };
  };
};

export function toBencherMetricFormat(
  result: BenchmarkResult,
): BencherMetricsFormat {
  // https://bencher.dev/docs/reference/bencher-metric-format/#bencher-metric-format-bmf-json-schema
  return {
    [result.name]: {
      throughput: {
        value: result.runTimesStatistics.meanMs,
        ['lower_value']: Math.min(...result.sortedRunTimesMs),
        ['upper_value']: Math.max(...result.sortedRunTimesMs),
      },
    },
  };
}
