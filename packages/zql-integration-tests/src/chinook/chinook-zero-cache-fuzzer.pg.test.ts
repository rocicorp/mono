/**
 * Protocol-client lanes of the zero-cache fuzzer: the view-syncer's downstream
 * stays query-equivalent to PostgreSQL through query churn and replicated writes.
 * See chinook-zero-cache-fuzzer.test.helpers.ts.
 */

import {expect} from 'vitest';
import {test, type PgTest} from '../../../zero-cache/src/test/db.ts';
import '../helpers/comparePg.ts';
import {
  PROTOCOL_QUERY_CASES,
  PROTOCOL_WRITE_FUZZ_CASES,
  PROTOCOL_WRITE_FUZZ_WRITE_COUNT,
  TIMEOUT_MS,
  albumTracksCase,
  checkProtocolWriteFuzzCases,
  deleteInsertedTrack,
  deleteTrack,
  expectProtocolCasesMatchPG,
  expectProtocolMatchesPG,
  insertTrack,
  moveTrackOutOfQuery,
  playlistTracksCase,
  startZeroCacheReplica,
  trackByIDCase,
  tracksInAlbumCase,
  waitForProtocolAfterReplica,
} from './chinook-zero-cache-fuzzer.test.helpers.ts';
import {checkQueryCases, panicIfFailed} from './fuzz/driver.ts';
import {builder} from './schema.ts';

test(
  `zero-cache protocol client stays query-equivalent to PostgreSQL for ${PROTOCOL_QUERY_CASES.length} generated query cases, ${PROTOCOL_WRITE_FUZZ_WRITE_COUNT} generated writes, and replicated writes`,
  {timeout: TIMEOUT_MS},
  async ({testDBs}: PgTest) => {
    const harness = await startZeroCacheReplica(testDBs, 'protocol');
    try {
      const client = await harness.startProtocolClient();
      await client.setQueries(
        [...PROTOCOL_QUERY_CASES, ...PROTOCOL_WRITE_FUZZ_CASES],
        'generated and write-fuzz query cases',
      );
      const report = await checkQueryCases(
        PROTOCOL_QUERY_CASES,
        async query => {
          await expectProtocolMatchesPG({...harness, client, query});
        },
      );
      expect(report.total).toBeGreaterThan(75);
      panicIfFailed(report, 8);

      expect(PROTOCOL_WRITE_FUZZ_CASES.length).toBeGreaterThan(5);
      const writeCount = await checkProtocolWriteFuzzCases(harness, client);
      expect(writeCount).toBe(PROTOCOL_WRITE_FUZZ_WRITE_COUNT);

      const query = builder.album
        .where('id', '=', 20)
        .related('tracks', t => t.orderBy('id', 'asc'))
        .one();

      await client.changeQueries({
        put: [{label: 'replicated writes', query}],
        label: 'replicated writes',
      });
      await expectProtocolMatchesPG({...harness, client, query});

      let baseline = await harness.watermark();
      await insertTrack(harness.upstream);
      let state = await harness.waitForReplicaVersion('track insert', baseline);
      if (state.watermark === undefined) {
        throw new Error('missing replica watermark after track insert');
      }
      await client.waitForCookieAtOrBeyond(state.watermark, 'track insert');
      await expectProtocolMatchesPG({...harness, client, query});

      baseline = await harness.watermark();
      await moveTrackOutOfQuery(harness.upstream);
      state = await harness.waitForReplicaVersion('track update', baseline);
      if (state.watermark === undefined) {
        throw new Error('missing replica watermark after track update');
      }
      await client.waitForCookieAtOrBeyond(state.watermark, 'track update');
      await expectProtocolMatchesPG({...harness, client, query});

      baseline = await harness.watermark();
      await deleteTrack(harness.upstream);
      state = await harness.waitForReplicaVersion('track delete', baseline);
      if (state.watermark === undefined) {
        throw new Error('missing replica watermark after track delete');
      }
      await client.waitForCookieAtOrBeyond(state.watermark, 'track delete');
      await expectProtocolMatchesPG({...harness, client, query});
    } finally {
      await harness.cleanup();
    }
  },
);

test(
  'zero-cache protocol client maintains multiple queries through churn and batched writes',
  {timeout: TIMEOUT_MS},
  async ({testDBs}: PgTest) => {
    const harness = await startZeroCacheReplica(testDBs, 'protocol');
    try {
      const client = await harness.startProtocolClient();
      const album20 = albumTracksCase(20);
      const album10 = albumTracksCase(10);
      const tracks20 = tracksInAlbumCase(20);
      const track108 = trackByIDCase(108);
      const track105 = trackByIDCase(105);
      const playlist1 = playlistTracksCase(1);
      const baseline = [
        {label: 'all-tracks', query: builder.track},
        {label: 'all-playlist-tracks', query: builder.playlistTrack},
      ];

      let active = [album20, album10, tracks20, track108];
      await client.setQueries(
        [...baseline, ...active],
        'initial overlapping query set',
      );
      await expectProtocolCasesMatchPG({harness, client, cases: active});

      let watermarkBaseline = await harness.watermark();
      await insertTrack(harness.upstream);
      await moveTrackOutOfQuery(harness.upstream);
      await waitForProtocolAfterReplica(
        harness,
        client,
        'batched track insert',
        watermarkBaseline,
      );
      await waitForProtocolAfterReplica(
        harness,
        client,
        'batched track move',
        watermarkBaseline,
      );
      await expectProtocolCasesMatchPG({harness, client, cases: active});

      await client.changeQueries({
        del: [album20, tracks20],
        put: [track105, playlist1],
        label: 'remove album 20 queries and add track 105 plus playlist 1',
      });
      active = [album10, track108, track105, playlist1];
      await expectProtocolCasesMatchPG({harness, client, cases: active});

      watermarkBaseline = await harness.watermark();
      await deleteInsertedTrack(harness.upstream);
      await deleteTrack(harness.upstream);
      await waitForProtocolAfterReplica(
        harness,
        client,
        'batched inserted track delete',
        watermarkBaseline,
      );
      await waitForProtocolAfterReplica(
        harness,
        client,
        'batched existing track delete',
        watermarkBaseline,
      );
      await expectProtocolCasesMatchPG({harness, client, cases: active});
    } finally {
      await harness.cleanup();
    }
  },
);
