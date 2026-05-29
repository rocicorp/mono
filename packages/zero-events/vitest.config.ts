import {newConfig} from 'shared/src/tool/vitest-config.ts';
import {mergeConfig} from 'vitest/config';

export default mergeConfig(newConfig(), {
  test: {
    browser: {enabled: false},
  },
});
