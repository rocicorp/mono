import {mergeConfig} from 'vitest/config';
import {benchConfig} from '../shared/src/tool/vitest-config.ts';

const config = mergeConfig(benchConfig, {
  test: {
    name: 'zero-cache/bench-pg',
    globalSetup: ['../zero-cache/test/pg-17.ts'],
    browser: {
      enabled: false,
    },
    testTimeout: 300_000,
    hookTimeout: 300_000,
  },
});

// Override include because mergeConfig merges arrays.
config.test.include = ['src/**/*.bench.pg.?(c|m)[jt]s?(x)'];

export default config;
