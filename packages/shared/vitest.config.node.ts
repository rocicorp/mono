import {mergeConfig} from 'vitest/config';
import {newConfig} from './src/tool/vitest-config.ts';

export default mergeConfig(newConfig(), {
  test: {
    name: 'shared/node',
    browser: {
      enabled: false,
    },
  },
});
