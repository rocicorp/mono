import {PG_LOCK_NOT_AVAILABLE} from '@drdgvhbh/postgres-error-codes';
import postgres from 'postgres';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {getConnectionURI, type PgTest, test} from '../../../test/db.ts';
import {pgClient, type PostgresDB} from '../../../types/pg.ts';
import type {ShardID} from '../../../types/shards.ts';
import {
  createReplicaAndSlot,
  createReplicationSlot,
  dropOldReplicasAndSlots,
  slotPoolSuffix,
} from './replication-slots.ts';
import {ensureGlobalTables, shardSetup} from './schema/shard.ts';

test.each([
  [0, 'a'],
  [1, 'b'],
  [25, 'z'],
  [26, 'aa'],
  [27, 'ab'],
  [26 * 2, 'ba'],
  [26 ** 2, 'za'],
  [26 ** 2 + 25, 'zz'],
  [26 ** 2 + 26, 'aaa'],
])('slotPoolSuffix: %d', (num, suffix) => {
  expect(slotPoolSuffix(num)).toBe(suffix);
});

describe('createReplicationSlot', () => {
  const APP_ID = 'zero';
  const SHARD_NUM = 18;
  const shard: ShardID = {appID: APP_ID, shardNum: SHARD_NUM};

  let upstream: PostgresDB;

  beforeEach<PgTest>(async ({testDBs}) => {
    upstream = await testDBs.create('replication_slots');
    await ensureGlobalTables(upstream, shard);
    await upstream.unsafe(
      shardSetup({...shard, publications: ['foo_pub', 'meta_pub']}, 'meta_pub'),
    );
    return () => testDBs.drop(upstream);
  });

  test('createReplicationSlot times out behind an older idle transaction', async () => {
    const lc = createSilentLogContext();
    const upstreamURI = getConnectionURI(upstream);
    const timeoutSlot = `${APP_ID}_${SHARD_NUM}_${Date.now()}_timeout`;
    const blocker = pgClient(lc, upstreamURI, 'slot-timeout-blocker', {
      max: 1,
    });
    const timeoutSession = pgClient(
      lc,
      upstreamURI,
      'slot-timeout-under-test',
      {
        max: 1,
        ['fetch_types']: false,
        connection: {replication: 'database'},
      },
    );

    let blockerInTransaction = false;
    try {
      // Start a transaction in one session and leave it open.
      await blocker`BEGIN`;
      blockerInTransaction = true;
      await blocker`SELECT txid_current()`;

      // The server-side lock_timeout (set inside createReplicationSlot)
      // should fire before the client-side orTimeout, producing a
      // PostgresError with code 55P03 (lock_not_available).
      let caught: unknown;
      try {
        await createReplicationSlot(lc, timeoutSession, {
          slotName: timeoutSlot,
          lockTimeout: 100,
        });
      } catch (e) {
        caught = e;
      }
      expect(caught).toBeInstanceOf(postgres.PostgresError);
      expect((caught as postgres.PostgresError).code).toBe(
        PG_LOCK_NOT_AVAILABLE,
      );
    } finally {
      if (blockerInTransaction) {
        await blocker`ROLLBACK`;
      }
      await blocker.end();
      await timeoutSession.end().catch(() => {});
      await upstream`
          SELECT pg_drop_replication_slot(slot_name)
            FROM pg_replication_slots
            WHERE slot_name = ${timeoutSlot} AND NOT active`;
    }
  });

  test('create replica and slots reuses pool names', async () => {
    const lc = createSilentLogContext();
    const upstreamURI = getConnectionURI(upstream);
    const session = pgClient(lc, upstreamURI, 'slot-pool', {
      max: 1,
      ['fetch_types']: false,
      connection: {replication: 'database'},
    });

    const create = (id: string) =>
      createReplicaAndSlot(lc, upstream, session, shard, id, false);

    let {slot_name: name} = await create('rep_1');
    expect(name).toBe('zero_18_a');

    ({slot_name: name} = await create('rep_2'));
    expect(name).toBe('zero_18_b');

    ({slot_name: name} = await create('rep_3'));
    expect(name).toBe('zero_18_c');

    await session`DROP_REPLICATION_SLOT zero_18_b`.simple();

    ({slot_name: name} = await create('rep_4'));
    expect(name).toBe('zero_18_b');

    expect(
      await upstream`SELECT id, slot, version FROM ${upstream(`${APP_ID}_${SHARD_NUM}`)}.replicas`,
    ).toMatchObject([
      {
        id: 'rep_1',
        slot: 'zero_18_a',
        version: /[a-z0-9]{5,}/,
      },
      {
        id: 'rep_2',
        slot: 'zero_18_b',
        version: /[a-z0-9]{5,}/,
      },
      {
        id: 'rep_3',
        slot: 'zero_18_c',
        version: /[a-z0-9]{5,}/,
      },
      {
        id: 'rep_4',
        slot: 'zero_18_b',
        version: /[a-z0-9]{5,}/,
      },
    ]);

    // Cleanup
    expect(await dropOldReplicasAndSlots(lc, upstream, shard, 100n)).toEqual({
      dropped: 3,
      active: 0,
      draining: 0,
    });
  });

  test('concurrent replica creation uses different slot names', async () => {
    const lc = createSilentLogContext();
    const upstreamURI = getConnectionURI(upstream);
    const sessions = Array.from({length: 3}, () =>
      pgClient(lc, upstreamURI, 'slot-pool', {
        max: 1,
        ['fetch_types']: false,
        connection: {replication: 'database'},
      }),
    );

    const results = await Promise.all(
      sessions.map((session, i) =>
        createReplicaAndSlot(lc, upstream, session, shard, `rep_${i}`, false),
      ),
    );
    const expectedSlots = new Set(['zero_18_a', 'zero_18_b', 'zero_18_c']);
    const names = new Set(results.map(({slot_name}) => slot_name));
    expect(names).toEqual(expectedSlots);

    const replicaSlots = await upstream<{slot: string}[]> /*sql*/ `
      SELECT slot FROM zero_18.replicas`.values();
    expect(new Set(replicaSlots.flat())).toEqual(expectedSlots);

    // Cleanup
    expect(await dropOldReplicasAndSlots(lc, upstream, shard, 100n)).toEqual({
      dropped: 3,
      active: 0,
      draining: 0,
    });
  });

  test('dropReplicaAndSlots', async () => {
    const lc = createSilentLogContext();
    const upstreamURI = getConnectionURI(upstream);
    const session = pgClient(lc, upstreamURI, 'slot-pool', {
      max: 1,
      ['fetch_types']: false,
      connection: {replication: 'database'},
    });
    await upstream`CREATE PUBLICATION foo_pub FOR ALL TABLES`;

    const create = (id: string) =>
      createReplicaAndSlot(lc, upstream, session, shard, id, false);

    await create('rep_1');
    await create('rep_2');
    await create('rep_3');

    // Subscribe to the second replication slot to prevent it from
    // being dropped.
    const stream = await session
      .unsafe(`
      START_REPLICATION SLOT zero_18_b LOGICAL 0/0 
        (proto_version '1', publication_names 'foo_pub');`)
      .readable();

    expect(await dropOldReplicasAndSlots(lc, upstream, shard, 3n)).toEqual({
      active: 1,
      draining: 1,
      dropped: 1,
    });

    stream.destroy();

    expect(await dropOldReplicasAndSlots(lc, upstream, shard, 4n)).toEqual({
      active: 0,
      draining: 0,
      dropped: 2,
    });
  });
});
