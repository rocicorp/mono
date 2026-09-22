/**
 * Replica lane of the zero-cache fuzzer: the replica stays query-equivalent to
 * PostgreSQL through generated writes and replicated writes. See
 * chinook-zero-cache-fuzzer.test.helpers.ts.
 */

import {expect} from 'vitest';
import {test, type PgTest} from '../../../zero-cache/src/test/db.ts';
import '../helpers/comparePg.ts';
import {
  TIMEOUT_MS,
  WRITE_FUZZ_CASES,
  WRITE_FUZZ_WRITE_COUNT,
  checkWriteFuzzCases,
  deleteTrack,
  expectReplicaMatchesPG,
  insertTrack,
  moveTrackOutOfQuery,
  startZeroCacheReplica,
} from './chinook-zero-cache-fuzzer.test.helpers.ts';
import {builder} from './schema.ts';

test(
  `zero-cache replica stays query-equivalent to PostgreSQL for ${WRITE_FUZZ_WRITE_COUNT} generated writes and replicated writes`,
  {timeout: TIMEOUT_MS},
  async ({testDBs}: PgTest) => {
    const harness = await startZeroCacheReplica(testDBs, 'writes');
    try {
      expect(WRITE_FUZZ_CASES.length).toBeGreaterThan(10);
      const writeCount = await checkWriteFuzzCases(harness);
      expect(writeCount).toBe(WRITE_FUZZ_WRITE_COUNT);

      const query = builder.album
        .where('id', '=', 20)
        .related('tracks', t => t.orderBy('id', 'asc'))
        .one();

      await expectReplicaMatchesPG({...harness, query});

      let baseline = await harness.watermark();
      await insertTrack(harness.upstream);
      await harness.waitForReplicaVersion('track insert', baseline);
      await expectReplicaMatchesPG({...harness, query});

      baseline = await harness.watermark();
      await moveTrackOutOfQuery(harness.upstream);
      await harness.waitForReplicaVersion('track update', baseline);
      await expectReplicaMatchesPG({...harness, query});

      baseline = await harness.watermark();
      await deleteTrack(harness.upstream);
      await harness.waitForReplicaVersion('track delete', baseline);
      await expectReplicaMatchesPG({...harness, query});
    } finally {
      await harness.cleanup();
    }
  },
);
