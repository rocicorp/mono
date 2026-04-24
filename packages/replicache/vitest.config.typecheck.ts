import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    name: 'replicache/typecheck',
    include: [],
    typecheck: {
      enabled: true,
      include: ['src/**/*.test-d.ts'],
    },
  },
});
