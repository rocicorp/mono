import {mergeConfig} from 'vitest/config';
import {benchConfig} from '../shared/src/tool/vitest-config.ts';

export default mergeConfig(benchConfig, {
  test: {
    name: 'zql-benchmarks/bench',
    globalSetup: ['../zero-cache/test/pg-17.ts'],
    browser: {
      enabled: false,
    },
    passWithNoTests: true,
  },
});
