import {defineWorkersConfig} from '@cloudflare/vitest-pool-workers/config';
import {defineWorkspace} from 'vitest/config';

export default defineWorkspace([
  defineWorkersConfig({
    test: {
      name: 'miniflare',
      include: ['src/**/*.*test.?(c|m)[jt]s?(x)'],
      poolOptions: {
        workers: {
          main: './test/miniflare-environment.js',
          miniflare: {
            compatibilityDate: '2024-04-05',
            compatibilityFlags: ['nodejs_compat'],
            durableObjects: {runnerDO: 'ServiceRunnerDO'},
          },
        },
      },
    },
  }),
  {
    test: {
      name: 'node',
      include: ['tool/*.test.ts'],
      environment: 'node',
    },
  },
  {
    test: {
      name: 'pg',
      include: ['src/**/*.pg-test.?(c|m)[jt]s?(x)'],
    },
  },
]);
