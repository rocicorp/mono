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
export async function runBenchmark(
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
