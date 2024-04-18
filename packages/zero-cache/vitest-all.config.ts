import {defineWorkersConfig} from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    include: ['src/**/*.{test,pg\\-test}.?(c|m)[jt]s?(x)'],
    poolOptions: {
      workers: {
        wrangler: {configPath: './test/wrangler.toml'},
      },
    },
  },
});
