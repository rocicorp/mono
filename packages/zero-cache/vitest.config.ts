import {defineConfig} from 'vitest/config';
import config from '../shared/src/tool/vitest-config.ts';

const {define, esbuild} = config;

const ci = process.env['CI'] === 'true' || process.env['CI'] === '1';

export default defineConfig({
  define,
  esbuild,
  test: {
    silent: true,
    coverage: {
      enabled: !ci, // Don't run coverage in continuous integration.
      reporter: [['html'], ['clover', {file: 'coverage.xml'}]],
      include: ['src/**'],
    },
  },
});
