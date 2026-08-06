import {defineConfig} from 'vitest/config';
import {configForCustomPg} from '../zero-cache/vitest.config.ts';

export default defineConfig({
  test: {
    projects: [
      'vitest.config.*.ts',
      '!vitest.config.bench.ts',
      '!vitest.config.bench.*.ts',
      ...configForCustomPg(import.meta.url),
    ],
  },
});
