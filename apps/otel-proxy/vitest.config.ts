import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    globals: true,
    // The public rocicorp/mono has no tests for this app
    // (tools/public-sync/public-filter).
    passWithNoTests: true,
  },
});
