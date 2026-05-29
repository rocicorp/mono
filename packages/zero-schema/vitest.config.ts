import config from 'shared/src/tool/vitest-config.ts';
import {defineConfig, mergeConfig} from 'vitest/config';

export default mergeConfig(
  config,
  defineConfig({
    test: {
      browser: {
        enabled: false,
      },
    },
  }),
);
