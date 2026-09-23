import {mergeConfig} from 'vitest/config';
import {configForNoPg} from './vitest.config.ts';

// The pipeline-driver suite a second time, with `deferIvmWrites` on: IVM
// derivation is held in an in-memory batch overlay and merged into each leaf
// scan, rather than written to the replica snapshot and rolled back.
//
// The mode has to be indistinguishable from write-through at every result this
// suite asserts. It is not free coverage of the source itself -- `zqlite-zql-
// test` runs the whole ZQL suite both ways for that -- it covers the seam
// above it: the diff probes the `prev` snapshot for unique-key conflicts, and
// with derivation deferred that snapshot no longer carries the advancement's
// own earlier changes, so `reconcilePendingConflicts` has to supply them.
const merged = mergeConfig(configForNoPg(import.meta.url), {
  test: {
    name: 'zero-cache/deferred-ivm-writes',
    env: {ZERO_TEST_DEFER_IVM_WRITES: '1'},
  },
});
// mergeConfig concatenates arrays; these need to replace.
merged.test.include = ['src/services/view-syncer/pipeline-driver.test.ts'];
merged.test.exclude = [];
export default merged;
