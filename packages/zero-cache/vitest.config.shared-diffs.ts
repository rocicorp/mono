import {mergeConfig} from 'vitest/config';
import {configForNoPg} from './vitest.config.ts';

// The pipeline-driver suites again, with the diffs of client groups replaying
// the segments of a worker-wide SharedDiffs where they can
// (shareSnapshotDiffs), and advancements cycling between deferred and
// written-through IVM writes as in vitest.config.mixed-ivm.ts, which decides
// how a replayed segment's displaced rows are read.
const merged = mergeConfig(configForNoPg(import.meta.url), {
  test: {
    name: 'zero-cache/shared-diffs',
    env: {ZERO_TEST_SHARED_DIFFS: '1', ZERO_TEST_DEFER_IVM_WRITES: 'mixed'},
  },
});
// mergeConfig concatenates arrays; these need to replace.
merged.test.include = ['src/services/view-syncer/pipeline-driver*.test.ts'];
merged.test.exclude = [];
export default merged;
