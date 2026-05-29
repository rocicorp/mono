import {benchConfig} from 'shared/src/tool/vitest-config.ts';
import {mergeConfig} from 'vitest/config';

export default mergeConfig(benchConfig, {
  test: {
    name: 'zero-cache/bench',
    browser: {
      enabled: false,
    },
  },
});
