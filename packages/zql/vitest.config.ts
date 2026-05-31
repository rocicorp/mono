import {defineConfig, mergeConfig} from 'vitest/config';
import {newConfig} from '../shared/src/tool/vitest-config.ts';

export default mergeConfig(
  newConfig(),
  defineConfig({
    test: {
      testTimeout: 20_000,
      browser: {
        enabled: false,
      },
    },
  }),
);
