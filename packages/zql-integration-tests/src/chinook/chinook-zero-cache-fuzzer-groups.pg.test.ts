/* oxlint-disable no-console */

/**
 * Client-group lane of the zero-cache fuzzer: client groups sharing a
 * production-configured sync worker stay query-equivalent to PostgreSQL
 * through generated writes, unique-key swaps, reconnects and view-syncer
 * restarts. See chinook-zero-cache-fuzzer-groups.test.helpers.ts.
 */

import {expect} from 'vitest';
import {test, type PgTest} from '../../../zero-cache/src/test/db.ts';
import '../helpers/comparePg.ts';
import {
  GROUPS_UPSTREAM_SETUP,
  SeededDeferredWritesBudget,
  checkClientGroupsFuzz,
} from './chinook-zero-cache-fuzzer-groups.test.helpers.ts';
import {
  FUZZ_SEED,
  TIMEOUT_MS,
  startZeroCacheReplica,
} from './chinook-zero-cache-fuzzer.test.helpers.ts';
import {formatSeed, fuzzBudget} from './fuzz/seed.ts';

/**
 * `ZERO_FUZZ_BUDGET` multiplies the swaps and the events of each kind, and
 * above 1 the lane uses every generated write instead of the protocol subset.
 */
const BUDGET = fuzzBudget();

test(
  'zero-cache client groups sharing a worker stay query-equivalent to PostgreSQL through writes, unique-key swaps, reconnects and restarts',
  {timeout: TIMEOUT_MS * BUDGET},
  async ({testDBs}: PgTest) => {
    const harness = await startZeroCacheReplica(
      testDBs,
      'groups',
      GROUPS_UPSTREAM_SETUP,
    );
    // Logged first, so a failing run shows the seed to replay it with.
    console.log(
      `client groups: ZERO_FUZZ_SEED=${formatSeed(FUZZ_SEED)} ZERO_FUZZ_BUDGET=${BUDGET}`,
    );
    const deferredWrites = new SeededDeferredWritesBudget(FUZZ_SEED);
    try {
      const stats = await checkClientGroupsFuzz(
        harness,
        FUZZ_SEED,
        BUDGET,
        deferredWrites,
      );
      console.log('client groups:', stats);
      expect(stats.writes).toBeGreaterThan(40);
      expect(stats.swaps).toBe(2 * BUDGET);
      // Every group was checked against a fresh hydration at the start, at
      // the end, and after each solo reconnect and restart.
      expect(stats.freshChecks).toBe(6 + 2 * BUDGET);
      // Each restart replaced the mixed group's view-syncer twice.
      expect(stats.starts['mixed']).toBe(1 + 2 * BUDGET);
      // Advancements applied their changes in every way.
      expect(stats.advancements.held).toBeGreaterThan(0);
      expect(stats.advancements.writtenThrough).toBeGreaterThan(0);
      expect(stats.advancements.switched).toBeGreaterThan(0);
    } finally {
      await harness.cleanup();
    }
    // Every view-syncer has stopped, and returned what it held.
    expect([deferredWrites.reservedRows, deferredWrites.heldBytes]).toEqual([
      0, 0,
    ]);
  },
);
