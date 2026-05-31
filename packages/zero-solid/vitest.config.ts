import solid from 'vite-plugin-solid';
import {defineConfig, mergeConfig} from 'vitest/config';
import {newConfig} from '../shared/src/tool/vitest-config.ts';

export default mergeConfig(
  newConfig(),
  defineConfig({
    plugins: [solid()],
    test: {
      environment: 'node',
    },
  }),
);
