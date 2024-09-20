import type {BenchmarkResult} from './benchmark.js';

export function formatAsReplicache(results: BenchmarkResult): string {
  const {name, runTimesStatistics: stats, sortedRunTimesMs: runs} = results;
  const f = (n: number) => n.toFixed(2);
  return `${name} 50/75/90/95%=${f(stats.medianMs)}/${f(stats.p75Ms)}/${f(
    stats.p90Ms,
  )}/${f(stats.p95Ms)} ms avg=${f(stats.meanMs)} ms (${
    runs.length
  } runs sampled)`;
}

export function formatAsBenchmarkJS(results: BenchmarkResult): string {
  const {name, runTimesStatistics: stats, sortedRunTimesMs: runs} = results;
  // Example:
  //   fib(20) x 11,465 ops/sec ±1.12% (91 runs sampled)
  //   createObjectBuffer with 200 comments x 81.61 ops/sec ±1.70% (69 runs sampled)
  const {medianMs} = stats;
  const value = results.byteSize
    ? formatToMBPerSecond(results.byteSize, medianMs)
    : `${((1 / medianMs) * 1000).toFixed(2)} ops/sec`;
  return `${name} x ${value} ${formatVariance(stats.variance)} (${
    runs.length
  } runs sampled)`;
}

export function formatVariance(variance: number): string {
  return `±${variance.toFixed(1)}%`;
}

function formatToMBPerSecond(size: number, timeMS: number): string {
  const bytes = (size / timeMS) * 1000;
  return (bytes / 2 ** 20).toFixed(2) + ' MB/s';
}
