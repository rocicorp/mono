import {mergeConfig} from 'vitest/config';
import {benchConfig} from './src/tool/vitest-config.ts';

export default mergeConfig(benchConfig, {
  test: {
    name: 'shared/bench/node',
    browser: {
      enabled: false,
    },
  },
});
