import {mergeConfig} from 'vitest/config';
import config from '../shared/src/tool/vitest-config.ts';

export default mergeConfig(config, {
  test: {
    name: 'analyze-query',
    browser: {
      // No need for browser tests yet
      enabled: false,
      name: '',
    },
  },
});
