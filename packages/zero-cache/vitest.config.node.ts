import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    name: 'node',
    include: ['src/**/*.test.?(c|m)[jt]s?(x)', 'tool/*.test.ts'],
    environment: 'node',
  },
});
