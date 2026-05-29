import {newConfig} from 'shared/src/tool/vitest-config.ts';
import {mergeConfig} from 'vitest/config';

export default mergeConfig(newConfig(), {
  test: {
    name: 'analyze-query',
    browser: {
      // No need for browser tests yet
      enabled: false,
      name: '',
    },
  },
});
