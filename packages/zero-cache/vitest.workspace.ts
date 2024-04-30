import {defineWorkersConfig} from '@cloudflare/vitest-pool-workers/config';
import {defineWorkspace} from 'vitest/config';

export default defineWorkspace([
  defineWorkersConfig({
    test: {
      include: ['src/**/*.test.?(c|m)[jt]s?(x)'],
      poolOptions: {
        workers: {
          main: './out/test/miniflare-environment.js',
          miniflare: {
            compatibilityDate: '2024-04-05',
            compatibilityFlags: ['nodejs_compat'],
            durableObjects: {runnerDO: 'ServiceRunnerDO'},
          },
        },
      },
      name: 'zero-cache',
    },
  }),
  {
    test: {
      include: ['tool/*.test.ts'],
      name: 'node',
      environment: 'node',
    },
  },
  {
    test: {
      include: ['src/**/*.pg-test.?(c|m)[jt]s?(x)'],
      name: 'pg',
    },
  },
]);
