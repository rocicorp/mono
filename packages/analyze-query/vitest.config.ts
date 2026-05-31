import {mergeConfig} from 'vitest/config';
import {newConfig} from '../shared/src/tool/vitest-config.ts';

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
