import * as mitata from 'mitata';
import type {TestAPI} from 'vitest';
import * as vitest from 'vitest';

export {do_not_optimize as use} from 'mitata';

declare module 'vitest' {
  export interface ProvidedContext {
    benchOutputFormat: 'json' | undefined;
  }
}

const benchOutputFormat = vitest.inject('benchOutputFormat');

type MitataBenchFn = Parameters<typeof mitata.bench>[1];

function benchAroundAll() {
  vitest.aroundAll(async runSuite => {
    await runSuite();
    const format =
      benchOutputFormat === 'json'
        ? {
            json: {
              samples: false,
              debug: false,
            },
          }
        : benchOutputFormat || 'mitata';

    await mitata.run({
      throw: true,
      format,
    });
  });
}

function wrapTest(testFn: (...args: any[]) => any): TestAPI {
  const wrapped = ((name: string, fn: MitataBenchFn) => {
    return testFn(name, () => {
      mitata.bench(name, fn);
    });
  }) as typeof vitest.test;

  for (const key of [
    'skip',
    'only',
    'todo',
    'shuffle',
    'concurrent',
    'sequential',
    'fails',
  ] as const) {
    Object.defineProperty(wrapped, key, {
      get: () => wrapTest((testFn as any)[key]),
    });
  }

  (wrapped as any).skipIf = (condition: any) =>
    wrapTest((testFn as any).skipIf(condition));
  (wrapped as any).runIf = (condition: any) =>
    wrapTest((testFn as any).runIf(condition));
  (wrapped as any).each = (testFn as any).each;
  (wrapped as any).for = (testFn as any).for;
  (wrapped as any).extend = (testFn as any).extend;

  return wrapped;
}

export const bench = wrapTest(vitest.test);

function wrapSuite(suiteFn: (...args: any[]) => any): typeof vitest.describe {
  const wrapped = ((...args: any[]) => {
    const [name, second, third] = args;
    const fn = typeof second === 'function' ? second : third;
    const options = typeof second === 'function' ? third : second;
    const wrappedFn = fn
      ? () => {
          benchAroundAll();
          return fn();
        }
      : fn;
    return typeof second === 'function'
      ? suiteFn(name, wrappedFn, options)
      : suiteFn(name, options, wrappedFn);
  }) as typeof vitest.describe;

  for (const key of [
    'skip',
    'only',
    'todo',
    'shuffle',
    'concurrent',
    'sequential',
  ] as const) {
    Object.defineProperty(wrapped, key, {
      get: () => wrapSuite((suiteFn as any)[key]),
    });
  }

  (wrapped as any).skipIf = (condition: any) =>
    wrapSuite((suiteFn as any).skipIf(condition));
  (wrapped as any).runIf = (condition: any) =>
    wrapSuite((suiteFn as any).runIf(condition));
  (wrapped as any).each = (suiteFn as any).each;
  (wrapped as any).for = (suiteFn as any).for;

  return wrapped;
}

export const describe = wrapSuite(vitest.describe);
