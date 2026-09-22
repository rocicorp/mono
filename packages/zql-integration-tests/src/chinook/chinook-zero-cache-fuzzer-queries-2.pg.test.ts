/**
 * Replica lane of the zero-cache fuzzer, part 2 of 3: the replica stays
 * query-equivalent to PostgreSQL for a slice of the generated query cases. The
 * slices are split across files so CI can spread them over test shards. See
 * chinook-zero-cache-fuzzer.test.helpers.ts.
 */

import {expect} from 'vitest';
import {test, type PgTest} from '../../../zero-cache/src/test/db.ts';
import '../helpers/comparePg.ts';
import {
  L1_QUERY_CASES,
  TIMEOUT_MS,
  ZERO_CACHE_QUERY_CASES,
  expectReplicaMatchesPG,
  startZeroCacheReplica,
} from './chinook-zero-cache-fuzzer.test.helpers.ts';
import {checkQueryCases, panicIfFailed} from './fuzz/driver.ts';

const PART = 2;
const PARTS = 3;
const total = ZERO_CACHE_QUERY_CASES.length;
const CASES = ZERO_CACHE_QUERY_CASES.slice(
  Math.floor((total * (PART - 1)) / PARTS),
  Math.floor((total * PART) / PARTS),
);

test(
  `zero-cache replica stays query-equivalent to PostgreSQL for ${CASES.length} of ${total} generated query cases (part ${PART}/${PARTS})`,
  {timeout: TIMEOUT_MS},
  async ({testDBs}: PgTest) => {
    const harness = await startZeroCacheReplica(testDBs, `queries_${PART}`);
    try {
      expect(L1_QUERY_CASES.coverage.fraction()).toBe(1);
      expect(total).toBeGreaterThan(500);
      const report = await checkQueryCases(CASES, query =>
        expectReplicaMatchesPG({...harness, query}),
      );
      expect(report.total).toBe(CASES.length);
      panicIfFailed(report, 12);
    } finally {
      await harness.cleanup();
    }
  },
);
