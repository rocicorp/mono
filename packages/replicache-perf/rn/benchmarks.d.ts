/**
 * Types for `benchmarks.js`, the bundle that `mono/packages/replicache-perf`
 * emits as `out/rn.js` and the runner copies in. Mirrors the exports of
 * `packages/replicache-perf/src/rn.ts`.
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

export declare const backends: readonly Backend[];
export declare const benchmarks: {name: string; group: string}[];

export declare function configure(opts: {
  backend: Backend;
  tmcwUrl: string;
}): void;
export declare function setBackend(backend: Backend): void;
export declare function setTmcwUrl(url: string): void;
export declare function formatAsReplicache(result: BenchmarkResult): string;
export declare function formatAsBenchmarkJS(result: BenchmarkResult): string;
export declare function findBenchmarks(
  groups: string[],
  runs: string[],
): {name: string; group: string}[];
export declare function runBenchmarkByNameAndGroup(
  name: string,
  group: string,
): Promise<['result', BenchmarkResult] | ['error', unknown] | undefined>;
