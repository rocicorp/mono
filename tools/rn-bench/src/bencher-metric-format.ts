import type {BenchmarkResult} from './benchmark.ts';

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
      // `latency`, not `throughput`: the value is a time in milliseconds, so
      // lower is better. Reported as throughput, Bencher reads the direction
      // backwards and every regression shows up as an improvement.
      latency: {
        value: result.runTimesStatistics.meanMs,
        ['lower_value']: Math.min(...result.sortedRunTimesMs),
        ['upper_value']: Math.max(...result.sortedRunTimesMs),
      },
    },
  };
}
