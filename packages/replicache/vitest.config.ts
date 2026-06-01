import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    projects: [
      'vitest.config.*.ts',
      '!vitest.config.bench.ts',
      '!vitest.config.bench.*.ts',
    ],
  },
});
