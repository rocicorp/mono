import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    include: ['src/**/*.pg\\-test.?(c|m)[jt]s?(x)'],
  },
});
