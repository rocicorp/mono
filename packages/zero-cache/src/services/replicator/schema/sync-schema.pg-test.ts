import {
  afterAll,
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
} from '@jest/globals';
import type postgres from 'postgres';
import {TestDBs, expectTables, initDB} from '../../../test/db.js';
import {createSilentLogContext} from '../../../test/logger.js';
import {initSyncSchema} from './sync-schema.js';

describe('replicator/sync-schema', () => {
  type Case = {
    name: string;

    upstreamSetup?: string;
    upstreamPreState?: Record<string, object[]>;
    upstreamPostState?: Record<string, object[]>;

    replicaSetup?: string;
    replicaPreState?: Record<string, object[]>;
    replicaPostState: Record<string, object[]>;
  };

  const cases: Case[] = [
    {
      name: 'sync schema meta',
      upstreamPostState: {
        ['zero.clients']: [],
      },
      replicaPostState: {
        ['zero.clients']: [],
        ['zero.schema_meta']: [
          {
            // Update these as necessary.
            version: 4,
            maxVersion: 4,
            minSafeRollbackVersion: 1,
            lock: 'v', // Internal column, always 'v'
          },
        ],
      },
    },
    {
      name: 'sync partially published upstream data',
      upstreamSetup: `
        CREATE TABLE unpublished(issue_id INTEGER, org_id INTEGER, PRIMARY KEY (org_id, issue_id));
        CREATE TABLE users(user_id INTEGER, password TEXT, handle TEXT, PRIMARY KEY (user_id));
        CREATE PUBLICATION zero_custom FOR TABLE users (user_id, handle);
    `,
      upstreamPreState: {
        users: [
          {userId: 123, password: 'not-replicated', handle: '@zoot'},
          {userId: 456, password: 'super-secret', handle: '@bonk'},
        ],
      },
      upstreamPostState: {
        ['zero.clients']: [],
      },
      replicaPostState: {
        ['zero.schema_meta']: [
          {
            // Update these as necessary.
            version: 4,
            maxVersion: 4,
            minSafeRollbackVersion: 1,
            lock: 'v', // Internal column, always 'v'
          },
        ],
        ['zero.clients']: [],
        users: [
          {userId: 123, handle: '@zoot', ['_0Version']: '00'},
          {userId: 456, handle: '@bonk', ['_0Version']: '00'},
        ],
      },
    },
  ];

  const testDBs = new TestDBs();
  let upstream: postgres.Sql;
  let replica: postgres.Sql;

  beforeEach(async () => {
    upstream = await testDBs.create('sync_schema_migration_upstream');
    replica = await testDBs.create('sync_schema_migration_replica');
  });

  afterEach(async () => {
    // Technically done by the tested code, but this helps clean things up in the event of failures.
    await replica.begin(async tx => {
      const subs =
        await tx`SELECT subname FROM pg_subscription WHERE subname = 'zero_sync'`;
      if (subs.count > 0) {
        await tx.unsafe(`
        ALTER SUBSCRIPTION zero_sync DISABLE;
        ALTER SUBSCRIPTION zero_sync SET(slot_name=NONE);
        DROP SUBSCRIPTION IF EXISTS zero_sync;
      `);
      }
    });
    await upstream.begin(async tx => {
      const slots = await tx`
        SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'zero_slot'`;
      if (slots.count > 0) {
        await tx`
          SELECT pg_drop_replication_slot('zero_slot');`;
      }
    });
    await testDBs.drop(upstream, replica);
  }, 10000);

  afterAll(async () => {
    await testDBs.end();
  });

  for (const c of cases) {
    test(
      c.name,
      async () => {
        await initDB(upstream, c.upstreamSetup, c.upstreamPreState);
        await initDB(replica, c.replicaSetup, c.replicaPreState);

        await initSyncSchema(
          createSilentLogContext(),
          replica,
          `postgres:///${upstream.options.database}`,
        );

        await expectTables(upstream, c.upstreamPostState);
        await expectTables(replica, c.replicaPostState);

        // Check that internal replication tables have been created.
        await expectTables(replica, {
          ['zero.tx_log']: [],
          ['zero.change_log']: [],
          ['zero.invalidation_registry']: [],
          ['zero.invalidation_index']: [],
        });

        // Subscriptions should have been dropped.
        const subs =
          await replica`SELECT subname FROM pg_subscription WHERE subname = 'zero_sync'`;
        expect(subs).toEqual([]);

        // Slot should still exist.
        const slots =
          await upstream`SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'zero_slot'`;
        expect(slots[0]).toEqual({slotName: 'zero_slot'});
      },
      10000,
    );
  }
});
