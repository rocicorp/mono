import {mergeConfig} from 'vitest/config';
import config from '../shared/src/tool/vitest-config.ts';

export default mergeConfig(config, {
  test: {
    name: 'replicache/browser',
    exclude: ['src/kv/sqlite*.ts'],
    benchmark: {
      exclude: ['src/kv/sqlite*.ts'],
    },
  },
});
