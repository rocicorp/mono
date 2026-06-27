import {mergeConfig} from 'vitest/config';
import {benchConfig} from '../shared/src/tool/vitest-config.ts';
import {ChangeIndex} from '../zql/src/ivm/change-index.ts';
import {ChangeType} from '../zql/src/ivm/change-type.ts';
import {SourceChangeIndex} from '../zql/src/ivm/source-change-index.ts';

// Like vitest.config.bench.ts but WITHOUT the Postgres globalSetup, so the
// pure in-memory IVM/ArrayView/debug benchmarks can run in environments without
// a container runtime. Only matches the memory/array-view/debug bench files.
export default mergeConfig(benchConfig, {
  define: {
    ...defineFromEnum('ChangeType', ChangeType),
    ...defineFromEnum('ChangeIndex', ChangeIndex),
    ...defineFromEnum('SourceChangeIndex', SourceChangeIndex),
  },

  test: {
    name: 'zql-benchmarks/bench-mem',
    include: [
      'src/ivm-memory.bench.ts',
      'src/memory-ivm-deopt.bench.ts',
      'src/array-view-relationships.bench.ts',
      'src/array-view-transaction.bench.ts',
      'src/debug-row-vended.bench.ts',
    ],
    browser: {
      enabled: false,
    },
    passWithNoTests: true,
  },
});

function defineFromEnum<Name extends string, E extends {[key: string]: number}>(
  name: Name,
  enumObj: E,
): {
  [K in keyof E as `${Name}.${string & K}`]: `${E[K]}`;
} {
  const result: Record<string, string> = {};
  for (const [key, value] of Object.entries(enumObj)) {
    result[`${name}.${key}`] = `${value}`;
  }
  return result as {
    [K in keyof E as `${Name}.${string & K}`]: `${E[K]}`;
  };
}
