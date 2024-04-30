import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    name: 'pg',
    include: ['src/**/*.pg-test.?(c|m)[jt]s?(x)'],
    retry: 3,
  },
});
