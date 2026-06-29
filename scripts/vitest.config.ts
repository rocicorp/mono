import {defineConfig, mergeConfig} from 'vitest/config';
import config from '../packages/shared/src/tool/vitest-config.ts';

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
