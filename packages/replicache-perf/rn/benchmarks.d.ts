/**
 * Types for `benchmarks.js`, the bundle that `mono/packages/replicache-perf`
 * emits as `out/rn.js` and the runner copies in. Only the exports this app
 * uses; rn.ts is the full surface.
 */
export type Backend = 'expo' | 'op' | 'mem';

export type BenchmarkResult = {
  name: string;
  group: string;
  byteSize?: number | undefined;
  sortedRunTimesMs: number[];
  runTimesStatistics: {
    meanMs: number;
    medianMs: number;
    p75Ms: number;
    p90Ms: number;
    p95Ms: number;
    variance: number;
  };
};

export declare function configure(opts: {
  backend: Backend;
  tmcwUrl: string;
}): void;
export declare function formatAsReplicache(result: BenchmarkResult): string;
export declare function runBenchmarkByNameAndGroup(
  name: string,
  group: string,
): Promise<['result', BenchmarkResult] | ['error', unknown] | undefined>;
