import {mergeConfig} from 'vitest/config';
import config from './src/tool/vitest-config.ts';

export default mergeConfig(config, {
  test: {
    name: 'shared/node',
    browser: {
      enabled: false,
      name: '', // not used but required by the type system
    },
  },
});
