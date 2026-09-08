import {type Benchmark, runBenchmark} from './benchmark.ts';

export type Harness = {
  /** Every benchmark this harness knows about. */
  readonly benchmarks: Benchmark[];
  findBenchmarks(groups: string[], runs: string[]): Benchmark[];
  runBenchmarkByNameAndGroup(
    name: string,
    group: string,
  ): Promise<['result', unknown] | ['error', unknown] | undefined>;
};

/**
 * The platform-neutral core shared by the browser entry point (perf.ts, which
 * registers every group) and the React Native one (rn.ts, which registers the
 * groups that run without a browser: `replicache` and `map-loop`). A group
 * belongs here only if it avoids IndexedDB, localStorage, `crypto.subtle` and
 * `TextEncoder`; see rn.ts for which of the remaining groups each rules out.
 */
export function makeHarness(benchmarks: Benchmark[]): Harness {
  function findBenchmark(name: string, group: string): Benchmark {
    for (const b of benchmarks) {
      if (b.name === name && b.group === group) {
        return b;
      }
    }
    throw new Error(`No benchmark named "${name}" in group "${group}"`);
  }

  return {
    benchmarks,

    findBenchmarks(groups: string[], runs: string[]): Benchmark[] {
      const bs = benchmarks.filter(b => groups.includes(b.group));
      if (runs.length === 0) {
        return bs;
      }
      const runRegExps = runs.map(r => new RegExp(r));
      return bs.filter(b => runRegExps.every(re => re.test(b.name)));
    },

    async runBenchmarkByNameAndGroup(name: string, group: string) {
      const b = findBenchmark(name, group);
      try {
        const result = await runBenchmark(b);
        if (!result) {
          return ['error', 'no result'] as const;
        }
        return ['result', result] as const;
      } catch (e) {
        return ['error', e] as const;
      }
    },
  };
}
