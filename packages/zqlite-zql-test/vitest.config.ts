import {defineConfig} from 'vitest/config';
import config from '../shared/src/tool/vitest-config.js';

const {define, esbuild} = config;

export default defineConfig({
  define,
  esbuild,
  test: {
    include: ['../zql/src/**/*.test.ts'],
    setupFiles: ['./src/setup.ts'],
    testTimeout: 20_000,
  },
});
