// oxlint-disable typescript/no-explicit-any
// oxlint-disable no-console

import {measure, type k_options as MeasureOptions} from 'mitata';
import type {TestAPI} from 'vitest';
import * as vitest from 'vitest';
import {
  printBenchHeader,
  printBenchResult,
  type Stats,
} from './bench-format.ts';

export type {MeasureOptions};

export {do_not_optimize as use} from 'mitata';

// These are replaced at build time by Vite's `define` block in benchConfig.
declare const process: {env: Record<string, string | undefined>};

const benchOutputFormat = process.env.BENCH_OUTPUT_FORMAT;
const colors = !process.env.NO_COLOR && !process.env.NODE_DISABLE_COLORS;

type MeasureFn = Parameters<typeof measure>[0];

const defaultMeasureOptions: MeasureOptions = {
  min_cpu_time: 2e9, // 2 seconds
  min_samples: 50,
};

type BenchResult = {name: string; stats: Stats};
const resultsStack: (BenchResult[] | undefined)[] = [];
let currentResults: BenchResult[] | undefined;

function benchResultToJson(name: string, stats: Stats) {
  return {
    name,
    stats: {
      min: stats.min,
      max: stats.max,
      avg: stats.avg,
      p75: stats.p75,
      p99: stats.p99,
    },
  };
}

function printResults(results: BenchResult[]) {
  const nameWidth = Math.max(20, ...results.map(r => r.name.length));
  console.log();
  printBenchHeader(nameWidth, colors);
  for (let i = 0; i < results.length; i++) {
    if (i > 0) console.log();
    const {name, stats} = results[i];
    printBenchResult(name, stats, nameWidth, colors);
  }
  console.log();
}

function printJsonResults(results: BenchResult[]) {
  console.log(
    JSON.stringify({
      benchmarks: results.map(({name, stats}) =>
        benchResultToJson(name, stats),
      ),
    }),
  );
}

// Copy vitest modifiers (skip, only, etc.) onto a wrapped function.
function copyModifiers<T extends (...args: any[]) => any>(
  wrapped: T,
  original: any,
  wrapFn: (fn: any) => any,
  extraKeys?: readonly string[],
): T {
  for (const key of [
    'skip',
    'only',
    'todo',
    'shuffle',
    'concurrent',
    'sequential',
    ...(extraKeys ?? []),
  ]) {
    Object.defineProperty(wrapped, key, {
      get: () => wrapFn((original as any)[key]),
    });
  }

  (wrapped as any).skipIf = (condition: any) =>
    wrapFn((original as any).skipIf(condition));
  (wrapped as any).runIf = (condition: any) =>
    wrapFn((original as any).runIf(condition));
  (wrapped as any).each = (original as any).each;
  (wrapped as any).for = (original as any).for;

  return wrapped;
}

function wrapTest(testFn: (...args: any[]) => any): TestAPI {
  const wrapped = ((name: string, fn: MeasureFn, opts?: MeasureOptions) =>
    testFn(name, async ({task}: {task: {fullName: string}}) => {
      const {fullName} = task;
      const stats = await measure(fn, {...defaultMeasureOptions, ...opts});

      if (currentResults) {
        currentResults.push({name: fullName, stats});
      } else if (benchOutputFormat === 'json') {
        console.log(JSON.stringify(benchResultToJson(fullName, stats)));
      } else {
        printBenchResult(fullName, stats, fullName.length, colors);
      }
    })) as typeof vitest.test;

  copyModifiers(wrapped, testFn, wrapTest, ['fails']);
  (wrapped as any).extend = (testFn as any).extend;

  return wrapped;
}

export const bench = wrapTest(vitest.test);

function wrapSuite(suiteFn: (...args: any[]) => any): typeof vitest.describe {
  const wrapped = ((...args: any[]) => {
    const [name, second, third] = args;
    const origFn = typeof second === 'function' ? second : third;
    const options = typeof second === 'function' ? third : second;
    const fn = () => {
      const results: BenchResult[] = [];

      vitest.beforeAll(() => {
        resultsStack.push(currentResults);
        currentResults = results;
      });

      vitest.afterAll(() => {
        currentResults = resultsStack.pop();
        if (results.length === 0) return;
        if (benchOutputFormat === 'json') {
          printJsonResults(results);
        } else {
          printResults(results);
        }
      });

      origFn();
    };
    return typeof second === 'function'
      ? suiteFn(name, fn, options)
      : suiteFn(name, options, fn);
  }) as typeof vitest.describe;

  copyModifiers(wrapped, suiteFn, wrapSuite);

  return wrapped;
}

export const describe = wrapSuite(vitest.describe);
