import {defineConfig} from 'vitest/config';
import {configForCustomPg} from '../zero-cache/vitest.config.ts';

export default defineConfig({
  test: {
    // A run-level option, so projects can't set it. See shared's vitest-config.
    passWithNoTests: true,
    projects: ['vitest.config.*.ts', ...configForCustomPg(import.meta.url)],
  },
});
