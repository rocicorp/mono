import {mergeConfig} from 'vitest/config';
import {newConfig} from '../shared/src/tool/vitest-config.ts';

export default mergeConfig(newConfig(), {
  test: {
    browser: {enabled: false},
  },
});
