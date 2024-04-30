import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    name: 'node',
    include: ['tool/*.test.ts'],
    environment: 'node',
  },
});
