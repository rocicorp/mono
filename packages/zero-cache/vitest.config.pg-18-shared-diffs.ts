import {mergeConfig} from 'vitest/config';
import {configForVersion} from './vitest.config.ts';

// The view-syncer suites again, with client groups replaying the segments of
// a worker-wide SharedDiffs where they can and advancements cycling between
// deferred and written-through IVM writes. See
// vitest.config.shared-diffs.ts. On the newest Postgres only, which is the
// one pull requests test against.
const merged = mergeConfig(configForVersion(18, import.meta.url), {
  test: {
    name: 'zero-cache/pg-18/shared-diffs',
    env: {ZERO_TEST_SHARED_DIFFS: '1', ZERO_TEST_DEFER_IVM_WRITES: 'mixed'},
  },
});
// mergeConfig concatenates arrays; these need to replace.
merged.test.include = ['src/services/view-syncer/**/*.pg.test.ts'];
merged.test.exclude = [];
export default merged;
