import {newConfig} from 'shared/src/tool/vitest-config.ts';
import {defineConfig, mergeConfig} from 'vitest/config';

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
