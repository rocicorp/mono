import {defineConfig} from 'vitest/config';

// Runs the ZQL suite in both `TableSource` modes, so a plain `vitest run` (with
// any filter or flags) covers write-through and deferred writes alike.
export default defineConfig({
  test: {
    projects: [
      'vitest.config.write-through.ts',
      'vitest.config.deferred-writes.ts',
    ],
  },
});
