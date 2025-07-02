import {defineConfig} from 'vitest/config';
import {configForCustomPg} from '../zero-cache/vitest.config.ts';

export default defineConfig({
  test: {
    projects: ['vitest.config.*.ts', ...configForCustomPg(import.meta.url)],
    testTimeout: 20_000, // 20 seconds
  },
});
