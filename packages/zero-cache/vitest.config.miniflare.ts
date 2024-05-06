import {defineWorkersConfig} from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    name: 'miniflare',
    include: ['src/**/*.*test.?(c|m)[jt]s?(x)'],
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
  },
});
