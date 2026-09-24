import {defineConfig} from 'vitest/config';
import config from '../shared/src/tool/vitest-config.ts';

const {define, esbuild} = config;

// The other half of the pair described in `vitest.config.write-through.ts`:
// the same suite with `TableSource` derivation held in a `PendingDelta` and
// merged into each leaf scan instead of written to the database it reads.
export default defineConfig({
  define,
  esbuild,
  test: {
    name: 'zqlite-zql-test-deferred-writes',
    include: ['../zql/src/**/*.test.ts'],
    setupFiles: ['./src/setup.ts'],
    testTimeout: 20_000,
    env: {
      ZQLITE_TEST_DEFER_WRITES: '1',
    },
  },
});
