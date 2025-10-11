import {mergeConfig} from 'vitest/config';
import config from '../shared/src/tool/vitest-config.ts';

export default mergeConfig(config, {
  test: {
    browser: {enabled: false},
  },
});
