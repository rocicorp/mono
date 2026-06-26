import {fileURLToPath} from 'node:url';
import {defineConfig} from 'vitest/config';

export default defineConfig({
  root: fileURLToPath(new URL('.', import.meta.url)),
  test: {
    browser: {
      enabled: false,
    },
    include: ['*.test.?(c|m)[jt]s?(x)'],
    name: 'github-scripts',
    silent: 'passed-only',
  },
});
