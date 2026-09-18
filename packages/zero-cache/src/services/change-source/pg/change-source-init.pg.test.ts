import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, vi} from 'vitest';
import {assert} from '../../../../../shared/src/asserts.ts';
import {TestLogSink} from '../../../../../shared/src/logging-test-utils.ts';
import {sleep} from '../../../../../shared/src/sleep.ts';
import {dropReplicationSlots, test, type PgTest} from '../../../test/db.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {Sink, Source} from '../../../types/streams.ts';
import {getSourceAndDestinationReplicas} from './change-source-init.ts';
import {subscribe, type StreamMessage} from './logical-replication/stream.ts';
import {toBigInt, toStateVersionString} from './lsn.ts';
import * as ReplicaStage from './schema/replica-stage-enum.ts';
import {
  createReplica,
  ensureGlobalTables,
  metadataPublicationName,
  shardSetup,
} from './schema/shard.ts';

describe(
  'change-source/pg getSourceAndDestinationReplicas',
  {timeout: 30000},
  () => {
    const APP_ID = 'zroz';
    const SHARD_NUM = 38;
    const shard = {appID: APP_ID, shardNum: SHARD_NUM};
    const schema = `${APP_ID}_${SHARD_NUM}`;
    const metadataPub = metadataPublicationName(APP_ID, SHARD_NUM);

    let lc: LogContext;
    let db: PostgresDB;
    let clientSeq = 0;

    beforeEach<PgTest>(async ({testDBs}) => {
      lc = new LogContext('warn', {}, new TestLogSink());
      db = await testDBs.create('change_source_init_test');
      clientSeq = 0;
      await ensureGlobalTables(db, shard);
      await db.unsafe(
        shardSetup({...shard, publications: [metadataPub]}, metadataPub),
      );

      return async () => {
        // Terminates the walsenders behind any active slots (source /
        // destination / resume reservations) so the slots can be dropped and
        // the drain loops below can exit.
        await dropReplicationSlots(db);
        await testDBs.drop(db);
      };
    });

    /**
     * Creates a logical slot with a completed (Replicate-stage) replica row and
     * opens a walsender session on it, so the slot is `active`. Returns handles
     * to advance its `confirmed_flush_lsn` via acks.
     */
    async function activeReplicateReplica(
      id: string,
      slot: string,
      generation?: string,
    ) {
      await db`SELECT pg_create_logical_replication_slot(${slot}, 'pgoutput')`;
      const [{lsn}] = await db<{lsn: string}[]>`
        SELECT confirmed_flush_lsn as lsn FROM pg_replication_slots
          WHERE slot_name = ${slot}`;
      const gen = generation ?? toStateVersionString(lsn);
      await createReplica(
        db,
        shard,
        id,
        slot,
        0,
        gen,
        {backupPath: id, backupV5: true},
        ReplicaStage.Replicate,
      );

      const sub = await subscribe(lc, db, slot, [metadataPub], toBigInt(lsn));
      const latest = {lsn: toBigInt(lsn)};
      drainTrackingLatest(sub.messages, latest);
      return {slot, acks: sub.acks, latest, generation: gen};
    }

    // An inactive (never-subscribed) completed replica, i.e. an orphan.
    async function inactiveReplicateReplica(
      id: string,
      slot: string,
      generation?: string,
    ) {
      await db`SELECT pg_create_logical_replication_slot(${slot}, 'pgoutput')`;
      const [{lsn}] = await db<{lsn: string}[]>`
        SELECT confirmed_flush_lsn as lsn FROM pg_replication_slots
          WHERE slot_name = ${slot}`;
      const gen = generation ?? toStateVersionString(lsn);
      await createReplica(
        db,
        shard,
        id,
        slot,
        0,
        gen,
        {backupPath: `${id}-backup`, backupV5: true},
        ReplicaStage.Replicate,
      );
      return {slot, generation: gen};
    }

    function drainTrackingLatest(
      messages: Source<StreamMessage>,
      latest: {lsn: bigint},
    ) {
      void (async () => {
        try {
          for await (const [msgLsn] of messages) {
            if (msgLsn > latest.lsn) {
              latest.lsn = msgLsn;
            }
          }
        } catch {
          // The stream errors when the slot's backend is terminated on
          // teardown; nothing to do.
        }
      })();
    }

    /**
     * Commits a published change (advancing WAL) and acks the latest LSN the
     * stream has observed, nudging `confirmed_flush_lsn` forward. Catch-up is
     * monotonic, so repeated calls reliably cross any fixed target LSN.
     */
    async function advance(source: {
      acks: Sink<bigint>;
      latest: {lsn: bigint};
    }) {
      const clientID = `c${clientSeq++}`;
      await db`
        INSERT INTO ${db(schema)}.clients
          ("clientGroupID", "clientID", "lastMutationID")
          VALUES ('g', ${clientID}, 0)`;
      await sleep(20); // allow the walsender to deliver the change
      source.acks.push(source.latest.lsn);
    }

    test('forks an active replica once it reaches the destination LSN', async () => {
      const src = await activeReplicateReplica('src', 'zro_0_src');

      // The destination slot is created at the current WAL head (ahead of the
      // source), so the fork can only happen after the source catches up.
      let settled = false;
      const resultP = getSourceAndDestinationReplicas(
        lc,
        db,
        shard,
        0,
        false,
        20, // pollIntervalMs
      );
      void resultP.then(() => {
        settled = true;
      });

      // Drive the source forward on each retry until the fork resolves.
      await vi.waitFor(
        async () => {
          if (!settled) {
            await advance(src);
            throw new Error('waiting for fork');
          }
        },
        {timeout: 20000, interval: 50},
      );

      const forked = await resultP;
      assert(forked, 'expected a fork result');
      // Restores from the source, replicates on a brand new replica.
      expect(forked.restoreFrom.id).toBe('src');
      expect(forked.replicateTo.id).not.toBe('src');
      // The new replica inherits the source's generation and starts in Restore.
      expect(forked.replicateTo.generation).toBe(src.generation);
      expect(forked.replicateTo.stage).toBe(ReplicaStage.Restore);
    });

    test('resumes an orphaned replica after the grace period', async () => {
      // A completed replica whose task has gone away: the slot is never
      // subscribed to, so it is inactive.
      await inactiveReplicateReplica('orphan', 'zro_0_orphan');

      const result = await getSourceAndDestinationReplicas(
        lc,
        db,
        shard,
        0,
        false,
        20, // pollIntervalMs
        50, // gracePeriodMs
      );

      assert(result, 'expected a resume result');
      // Resumption reuses the same replica rather than creating a new one, and
      // claiming it transitions it to the Restore stage.
      expect(result.restoreFrom.id).toBe('orphan');
      expect(result.replicateTo.id).toBe('orphan');
      expect(result.replicateTo.stage).toBe(ReplicaStage.Restore);
    });

    test('prefers forking an active replica over resuming an orphan', async () => {
      // An orphan and an active sibling of the *same* generation (so both are
      // restore candidates). Even with a tiny grace, forking the active
      // replica should win over resuming the inactive orphan.
      const generation = 'ggggggggg';
      await inactiveReplicateReplica('orphan', 'zro_0_orphan', generation);
      const src = await activeReplicateReplica('src', 'zro_0_src', generation);

      let settled = false;
      const resultP = getSourceAndDestinationReplicas(
        lc,
        db,
        shard,
        0,
        false,
        20, // pollIntervalMs
        1, // tiny grace: the orphan is immediately resumable
      );
      void resultP.then(() => {
        settled = true;
      });

      await vi.waitFor(
        async () => {
          if (!settled) {
            await advance(src);
            throw new Error('waiting for fork');
          }
        },
        {timeout: 20000, interval: 50},
      );

      const forked = await resultP;
      assert(forked, 'expected a fork result');
      // Forked the active source, not resumed the orphan.
      expect(forked.restoreFrom.id).toBe('src');
    });

    test('returns undefined when there are no candidates', async () => {
      expect(
        await getSourceAndDestinationReplicas(lc, db, shard, 0, false, 20),
      ).toBeUndefined();
    });
  },
);
