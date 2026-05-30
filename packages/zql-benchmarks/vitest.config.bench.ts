import {mergeConfig} from 'vitest/config';
import {benchConfig} from '../shared/src/tool/vitest-config.ts';
import {ChangeIndex} from '../zql/src/ivm/change-index.ts';
import {ChangeType} from '../zql/src/ivm/change-type.ts';
import {SourceChangeIndex} from '../zql/src/ivm/source-change-index.ts';

export default mergeConfig(benchConfig, {
  define: {
    // These are hot and in benchmarks Vitest doesn't do a good job of inlining
    // them, so we inline them ourselves here.
    ...defineFromEnum('ChangeType', ChangeType),
    ...defineFromEnum('ChangeIndex', ChangeIndex),
    ...defineFromEnum('SourceChangeIndex', SourceChangeIndex),
  },

  test: {
    name: 'zql-benchmarks/bench',
    globalSetup: ['../zero-cache/test/pg-17.ts'],
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
