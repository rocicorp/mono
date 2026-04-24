import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    exclude: ['**/*.test.ts', 'node_modules/**'],
    typecheck: {
      enabled: true,
      include: ['src/**/*.test-d.ts'],
    },
  },
});
