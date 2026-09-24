import {mergeConfig} from 'vitest/config';
import {configForNoPg} from './vitest.config.ts';

// The pipeline-driver suite a second time, with `deferIvmWrites` off: IVM
// derivation is written to the replica snapshot and rolled back, rather than
// held in an in-memory batch overlay and merged into each leaf scan (the
// default, which the other configs run).
//
// The two modes have to be indistinguishable at every result this suite
// asserts. Advancements also write through when they do not fit in the
// worker's deferred writes budget, so this path stays in use with the flag on.
const merged = mergeConfig(configForNoPg(import.meta.url), {
  test: {
    name: 'zero-cache/write-through-ivm-writes',
    env: {ZERO_TEST_DEFER_IVM_WRITES: '0'},
  },
});
// mergeConfig concatenates arrays; these need to replace.
merged.test.include = ['src/services/view-syncer/pipeline-driver.test.ts'];
merged.test.exclude = [];
export default merged;
