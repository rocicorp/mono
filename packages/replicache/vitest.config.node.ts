import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    name: 'replicache/node',
    browser: {
      enabled: false,
    },
    include: ['src/kv/sqlite*.test.ts'],
    benchmark: {
      include: ['src/kv/sqlite*.bench.ts'],
    },
    typecheck: {
      enabled: false,
    },
  },
});
