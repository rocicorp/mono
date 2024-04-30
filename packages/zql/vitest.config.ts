import {defineConfig, mergeConfig} from 'vitest/config';
import {config} from '../shared/src/tool/vitest-config.js';

export default mergeConfig(
  config,
  defineConfig({
    test: {
      testTimeout: 20_000,
    },
  }),
);
