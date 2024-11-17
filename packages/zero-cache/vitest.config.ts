import {defineConfig} from 'vitest/config';
import {config} from '../shared/src/tool/vitest-config.js';

const {define, esbuild} = config;

export default defineConfig({
  define,
  esbuild,
  test: {
    reporters: 'basic',
    coverage: {
      enabled: true,
      reporter: [['html'], ['clover', {file: 'coverage.xml'}]],
      include: ['src/**'],
    },
  },
});
