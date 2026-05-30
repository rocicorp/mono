import {mergeConfig} from 'vitest/config';
import {benchConfig} from '../shared/src/tool/vitest-config.ts';

export default mergeConfig(benchConfig, {
  test: {
    projects: ['./vitest.config.bench.web.ts'],
  },
});
