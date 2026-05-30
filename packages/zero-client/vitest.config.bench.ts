import {mergeConfig} from 'vitest/config';
import {benchConfig} from '../shared/src/tool/vitest-config.ts';

export default mergeConfig(benchConfig, {
  test: {
    name: 'zero-client/bench',
    browser: {
      enabled: false,
    },
  },
});
