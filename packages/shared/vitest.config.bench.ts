import {mergeConfig} from 'vitest/config';
import {benchConfig} from './src/tool/vitest-config.ts';

export default mergeConfig(benchConfig, {
  test: {
    projects: ['./vitest.config.bench.*.ts'],
    silent: false,
    disableConsoleIntercept: false,
  },
});
