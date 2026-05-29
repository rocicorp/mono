import {newConfig} from 'shared/src/tool/vitest-config.ts';
import solid from 'vite-plugin-solid';
import {defineConfig, mergeConfig} from 'vitest/config';

export default mergeConfig(
  newConfig(),
  defineConfig({
    plugins: [solid()],
    test: {
      environment: 'node',
    },
  }),
);
