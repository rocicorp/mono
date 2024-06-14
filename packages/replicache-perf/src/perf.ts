import {benchmarks as compareBenchmarks} from './benchmarks/compare-utf8.js';
import {benchmarks as hashBenchmarks} from './benchmarks/hash.js';
import {benchmarkIDBRead, benchmarkIDBWrite} from './benchmarks/idb.js';
import {benchmarks as replicacheBenchmarks} from './benchmarks/replicache.js';
import {benchmarks as storageBenchmarks} from './benchmarks/storage.js';
import {benchmarks as uuidBenchmarks} from './benchmarks/uuid.js';
import type {RandomDataType} from './data.js';
import {
  formatAsBenchmarkJS,
  formatAsReplicache,
  formatVariance,
} from './format.js';
import {benchmarks as mapLoopBenchmarks} from './map-loop.js';

export type Benchmark = {
  name: string;
  group: string;
  byteSize?: number | undefined;
  skip?: (() => Promise<boolean> | boolean) | undefined;
  setup?: (() => Promise<void> | void) | undefined;
  setupEach?: (() => Promise<void> | void) | undefined;
  teardown?: (() => Promise<void> | void) | undefined;
  teardownEach?: (() => Promise<void> | void) | undefined;
  run: (b: Bencher, i: number) => Promise<void> | void;
};

export type Bencher = {
  reset: () => void;
  stop: () => void;
  subtract: (a: number) => void;
};

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

async function runBenchmark(
  benchmark: Benchmark,
): Promise<BenchmarkResult | undefined> {
  // Execute fn at least this many runs.
  const minRuns = 9;
  const maxRuns = 21;
  // Execute fn at least for this long.
  const minTime = 500;
  const maxTotalTime = 5000;
  const times: number[] = [];
  let sum = 0;

  if (benchmark.skip && (await benchmark.skip())) {
    return;
  }

  try {
    await benchmark.setup?.();

    const totalTimeStart = performance.now();
    for (let i = 0; (i < minRuns || sum < minTime) && i < maxRuns; i++) {
      if (i >= minRuns && performance.now() - totalTimeStart > maxTotalTime) {
        break;
      }

      try {
        await benchmark.setupEach?.();

        let t0 = 0;
        let t1 = 0;
        let subtracted = 0;
        const reset = () => {
          performance.mark('mark-' + i);
          t0 = performance.now();
          subtracted = 0;
        };
        const stop = () => {
          t1 = performance.now();
          performance.measure(benchmark.name, 'mark-' + i);
        };
        const subtract = (n: number) => {
          subtracted += n;
        };
        reset();

        await benchmark.run(
          {
            reset,
            stop,
            subtract,
          },
          i,
        );
        if (t1 === 0) {
          stop();
        }
        const dur = t1 - t0 - subtracted;
        times.push(dur);
        sum += dur;
      } finally {
        await benchmark.teardownEach?.();
      }
    }
  } finally {
    await benchmark.teardown?.();
  }

  times.sort((a, b) => a - b);
  // Remove two slowest. Treat them as JIT warmup.
  times.splice(0, 2);
  const calcPercentile = (percentile: number): number =>
    times[Math.floor((runCount * percentile) / 100)];
  const runCount = times.length;
  const medianMs = calcPercentile(50);
  return {
    name: benchmark.name,
    group: benchmark.group,
    byteSize: benchmark.byteSize,
    sortedRunTimesMs: times,
    runTimesStatistics: {
      meanMs: sum / runCount,
      medianMs,
      p75Ms: calcPercentile(75),
      p90Ms: calcPercentile(90),
      p95Ms: calcPercentile(95),
      variance: Math.max(
        medianMs - times[0],
        times[times.length - 1] - medianMs,
      ),
    },
  };
}

export const benchmarks = [
  ...replicacheBenchmarks(),
  ...hashBenchmarks(),
  ...storageBenchmarks(),
  ...compareBenchmarks(),
  ...uuidBenchmarks(),
  ...mapLoopBenchmarks(),
];

for (const b of [benchmarkIDBRead, benchmarkIDBWrite]) {
  for (const numKeys of [1, 10, 100, 1000]) {
    const dataTypes: RandomDataType[] = ['string', 'object', 'arraybuffer'];
    for (const dataType of dataTypes) {
      const kb = 1024;
      const mb = kb * kb;
      const sizes = [
        kb,
        32 * kb,
        // Note: on blink, as of 4/2/2021, there's a cliff at 64kb
        mb,
        10 * mb,
        100 * mb,
      ];
      const group = dataType === 'arraybuffer' ? 'idb' : 'idb-extras';
      for (const valSize of sizes) {
        if (valSize > 10 * mb) {
          if (numKeys > 1) {
            continue;
          }
        } else if (valSize >= mb) {
          if (numKeys > 10) {
            continue;
          }
        }

        benchmarks.push(b({group, dataType, numKeys, valSize}));
      }
    }
  }
}

function findBenchmark(name: string, group: string): Benchmark {
  for (const b of benchmarks) {
    if (b.name === name && b.group === group) {
      return b;
    }
  }
  throw new Error(`No benchmark named "${name}" in group "${group}"`);
}

export async function runBenchmarkByNameAndGroup(
  name: string,
  group: string,
  format: 'replicache' | 'benchmarkJS',
): Promise<{jsonEntries: Entry[]; text: string} | {error: string} | undefined> {
  const b = findBenchmark(name, group);
  try {
    const result = await runBenchmark(b);
    if (!result) {
      return undefined;
    }
    return {
      jsonEntries: createGithubActionBenchmarkJSONEntries(result),
      text:
        format === 'replicache'
          ? formatAsReplicache(result)
          : formatAsBenchmarkJS(result),
    };
  } catch (e) {
    return {error: `${b.name} had an error: ${e}`};
  }
}

export function findBenchmarks(groups: string[]): Benchmark[] {
  return benchmarks.filter(b => groups.includes(b.group));
}

export async function runAll(groups: string[]): Promise<void> {
  const out: HTMLElement | null = document.getElementById('out');
  if (!out) {
    return;
  }
  const benchmarks = findBenchmarks(groups);
  for (const b of benchmarks) {
    try {
      const result = await runBenchmark(b);
      if (result) {
        out.textContent += formatAsReplicache(result) + '\n';
      }
    } catch (e) {
      out.textContent += `${b.name} had an error: ${e}` + '\n';
    }
  }
  out.textContent += 'Done!\n';
}

// See https://github.com/benchmark-action/github-action-benchmark#examples
type Entry = {
  name: string;
  unit: string;
  value: number;
  // variance
  range?: string;
  // any extra info, will be displayed in tool tip on graphs
  extra?: string;
};

function createGithubActionBenchmarkJSONEntries(
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
