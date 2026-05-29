import config from 'shared/src/tool/vitest-config.ts';
import {mergeConfig} from 'vitest/config';

export default mergeConfig(config, {
  test: {
    browser: {enabled: false},
  },
});
