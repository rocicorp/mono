import {mergeConfig} from 'vitest/config';
import config from '../../packages/shared/src/tool/vitest-config.ts';

export default mergeConfig(config, {
  test: {
    name: 'ast-to-zql',
    browser: {
      // No need for browser tests yet
      enabled: false,
      name: '',
    },
  },
});
