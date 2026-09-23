import {mergeConfig} from 'vitest/config';
import {configForVersion} from './vitest.config.ts';

// The view-syncer suites again, with every other advancement's IVM derivation
// held in an in-memory batch overlay and the rest written through to the
// replica snapshot. See vitest.config.mixed-ivm.ts. On the newest Postgres
// only, which is the one pull requests test against.
const merged = mergeConfig(configForVersion(18, import.meta.url), {
  test: {
    name: 'zero-cache/pg-18/mixed-ivm-writes',
    env: {ZERO_TEST_DEFER_IVM_WRITES: 'mixed'},
  },
});
// mergeConfig concatenates arrays; these need to replace.
merged.test.include = ['src/services/view-syncer/**/*.pg.test.ts'];
merged.test.exclude = [];
export default merged;
