import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, vi} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../../../shared/src/logging-test-utils.ts';
import {test, type PgTest} from '../../../test/db.ts';
import {PG_15, PG_17} from '../../../types/pg-versions.ts';
import {type PostgresDB} from '../../../types/pg.ts';
import type {ShardID} from '../../../types/shards.ts';
import {ReplicationSlotCleanupMonitor} from './replication-slot-cleanup-monitor.ts';
import {
  createReplicaAndSlot,
  type ReplicationSlotResult,
} from './replication-slots.ts';
import {
  ensureGlobalTables,
  metadataPublicationName,
  shardSetup,
} from './schema/shard.ts';

describe('ReplicationSlotCleanupMonitor', () => {
  // Note: use a slot-pool prefix unique to this file. Replication slot names
  // are cluster-wide unique, so sharing e.g. `zero_18_*` with another pg test
  // file causes cross-file slot clobbering when vitest runs them in parallel.
  const APP_ID = 'slotcln';
  const SHARD_NUM = 18;
  const shard: ShardID = {appID: APP_ID, shardNum: SHARD_NUM};

  let upstream: PostgresDB;
  let pgVersion: number;
  const monitors: ReplicationSlotCleanupMonitor[] = [];

  beforeEach<PgTest>(async ({testDBs}) => {
    upstream = await testDBs.create('replication_slot_cleanup');
    [{pgVersion}] =
      await upstream`SELECT current_setting('server_version_num')::int as "pgVersion"`;
    await ensureGlobalTables(upstream, shard);
    const metadataPub = metadataPublicationName(APP_ID, SHARD_NUM);
    await upstream.unsafe(
      shardSetup(
        {...shard, publications: ['foo_pub', metadataPub]},
        metadataPub,
      ),
    );
    return async () => {
      monitors.forEach(m => m.stop());
      monitors.length = 0;
      await testDBs.drop(upstream);
    };
  });

  const results: ReplicationSlotResult<string>[] = [];
  // Creates a replica + slot (choosing the next name from the shard's pool),
  // whose replication session is initially active (i.e. `active = true`).
  async function createSlot(id: string) {
    const result = await createReplicaAndSlot(
      createSilentLogContext(),
      upstream,
      'initial-sync',
      shard,
      id,
      false,
      {backupPath: id, backupV5: true},
      snapshot => Promise.resolve(`captured(${snapshot})`),
    );
    results.push(result);
    return result;
  }

  async function waitForInactive(slot: string) {
    await vi.waitFor(async () => {
      const [row] = await upstream<{active: boolean}[]>`
        SELECT active FROM pg_replication_slots WHERE slot_name = ${slot}`;
      expect(row?.active).toBe(false);
    });
  }

  beforeEach(() => {
    results.length = 0;
  });

  test('getSlotsToCleanup tracks inactivity across polls (manual, PG <17 path)', async () => {
    // 'a' stays active (the current slot), 'b' goes inactive.
    await createSlot('rep_a');
    const other = await createSlot('rep_b');
    other.initialSession.destroy();
    await waitForInactive('slotcln_18_b');

    // Force the manual-tracking path regardless of the server version.
    const monitor = new ReplicationSlotCleanupMonitor(
      createSilentLogContext(),
      shard,
      upstream,
      PG_15,
      'slotcln_18_a',
      {inactiveSlotCleanupTimeoutMs: 60_000},
    );

    // The first poll only records when the slot was first observed inactive.
    const t0 = new Date(10_000_000);
    expect(await monitor.getSlotsToCleanup(t0)).toEqual([]);

    // Still within the timeout window: not yet eligible.
    expect(
      await monitor.getSlotsToCleanup(new Date(t0.getTime() + 59_999)),
    ).toEqual([]);

    // Once the timeout has elapsed, the inactive slot is eligible.
    expect(
      await monitor.getSlotsToCleanup(new Date(t0.getTime() + 60_000)),
    ).toEqual(['slotcln_18_b']);

    // The active current slot is never eligible, no matter how much time
    // passes.
    expect(
      await monitor.getSlotsToCleanup(new Date(t0.getTime() + 60 * 60_000)),
    ).toEqual(['slotcln_18_b']);
  });

  test('getSlotsToCleanup uses inactive_since (native, PG 17+ path)', async () => {
    if (pgVersion < PG_17) {
      return; // inactive_since only exists on PG 17+
    }
    await createSlot('rep_a');
    const other = await createSlot('rep_b');
    other.initialSession.destroy();
    await waitForInactive('slotcln_18_b');

    // A tiny timeout: the slot has been inactive "long enough".
    const eager = new ReplicationSlotCleanupMonitor(
      createSilentLogContext(),
      shard,
      upstream,
      PG_17,
      'slotcln_18_a',
      {inactiveSlotCleanupTimeoutMs: 0},
    );
    expect(await eager.getSlotsToCleanup(new Date())).toEqual(['slotcln_18_b']);

    // A long timeout: the slot has not been inactive long enough yet.
    const patient = new ReplicationSlotCleanupMonitor(
      createSilentLogContext(),
      shard,
      upstream,
      PG_17,
      'slotcln_18_a',
      {inactiveSlotCleanupTimeoutMs: 60 * 60_000},
    );
    expect(await patient.getSlotsToCleanup(new Date())).toEqual([]);
  });

  test('disables cleanup when the current slot is not active', async () => {
    // Set up an OTHER slot that would otherwise be eligible for cleanup...
    const other = await createSlot('rep_a'); // slotcln_18_a
    other.initialSession.destroy();
    await waitForInactive('slotcln_18_a');

    // ...but the current slot ('b') is also inactive, so cleanup is disabled.
    const current = await createSlot('rep_b'); // slotcln_18_b
    current.initialSession.destroy();
    await waitForInactive('slotcln_18_b');

    const sink = new TestLogSink();
    const monitor = new ReplicationSlotCleanupMonitor(
      new LogContext('debug', {}, sink),
      shard,
      upstream,
      PG_15,
      'slotcln_18_b', // current slot
      {inactiveSlotCleanupTimeoutMs: 0},
    );

    // Even though 'a' is long-inactive, nothing is returned because the
    // current slot is not active.
    expect(await monitor.getSlotsToCleanup(new Date(10_000_000))).toEqual([]);
    expect(
      sink.messages.some(([, , args]) =>
        String(args[0]).includes('current slot slotcln_18_b is not active'),
      ),
    ).toBe(true);
  });

  test('start() drops inactive slots and their replica rows', async () => {
    // 'a' stays active (the current slot), 'b' goes inactive.
    await createSlot('rep_a');
    const other = await createSlot('rep_b');
    other.initialSession.destroy();
    await waitForInactive('slotcln_18_b');

    const monitor = new ReplicationSlotCleanupMonitor(
      createSilentLogContext(),
      shard,
      upstream,
      PG_15,
      'slotcln_18_a',
      // Zero timeout so the second poll (once the slot's inactivity has been
      // observed once) treats it as eligible; poll quickly to keep the test
      // fast.
      {inactiveSlotCleanupTimeoutMs: 0, pollIntervalMs: 20},
    );
    monitors.push(monitor);
    monitor.start();

    // The inactive slot and its replica row are eventually cleaned up.
    await vi.waitFor(async () => {
      const slots = await upstream<{slot: string}[]>`
        SELECT slot_name as slot FROM pg_replication_slots
          WHERE slot_name LIKE 'slotcln_18_%' ORDER BY slot_name`.values();
      expect(slots).toEqual([['slotcln_18_a']]);

      const replicas = await upstream<{slot: string}[]>`
        SELECT slot FROM ${upstream(`${APP_ID}_${SHARD_NUM}`)}.replicas
          ORDER BY slot`.values();
      expect(replicas).toEqual([['slotcln_18_a']]);
    });

    monitor.stop();
  });
});
