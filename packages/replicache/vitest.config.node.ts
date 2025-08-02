import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    name: 'replicache/node',
    browser: {
      enabled: false,
    },
    include: ['src/**/*.{test,spec}.node.?(c|m)[jt]s?(x)'],
    typecheck: {
      enabled: false,
    },
    benchmark: {
      include: ['src/**/*.{bench,benchmark}.node.?(c|m)[jt]s?(x)'],
    },
  },
});
