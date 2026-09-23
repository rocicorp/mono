import {mergeConfig} from 'vitest/config';
import {configForNoPg} from './vitest.config.ts';

// The pipeline-driver suite again, with every other advancement's IVM
// derivation held in an in-memory batch overlay and the rest written through
// to the replica snapshot, as happens when an advancement does not fit in the
// worker's deferred writes budget. Drivers switch between the two modes from
// one advancement to the next, and drivers that share a budget (and a row
// cache) run in different modes at the same version.
const merged = mergeConfig(configForNoPg(import.meta.url), {
  test: {
    name: 'zero-cache/mixed-ivm-writes',
    env: {ZERO_TEST_DEFER_IVM_WRITES: 'mixed'},
  },
});
// mergeConfig concatenates arrays; these need to replace.
merged.test.include = ['src/services/view-syncer/pipeline-driver.test.ts'];
merged.test.exclude = [];
export default merged;
