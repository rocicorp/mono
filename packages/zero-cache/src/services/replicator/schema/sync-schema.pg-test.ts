import type postgres from 'postgres';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {
  dropReplicationSlot,
  expectTables,
  getConnectionURI,
  initDB,
  testDBs,
} from '../../../test/db.js';
import {replicationSlot} from '../initial-sync.js';
import {initSyncSchema} from './sync-schema.js';

const REPLICA_ID = 'sync_schema_test_id';

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
      name: 'sync schema versions',
      upstreamPostState: {
        ['zero.clients']: [],
      },
      replicaPostState: {
        ['zero.clients']: [],
        ['_zero.SchemaVersions']: [
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
        CREATE TABLE users("userID" INTEGER, password TEXT, handle TEXT, PRIMARY KEY ("userID"));
        CREATE PUBLICATION zero_custom FOR TABLE users ("userID", handle);
    `,
      upstreamPreState: {
        users: [
          {userID: 123, password: 'not-replicated', handle: '@zoot'},
          {userID: 456, password: 'super-secret', handle: '@bonk'},
        ],
      },
      upstreamPostState: {
        ['zero.clients']: [],
      },
      replicaPostState: {
        ['_zero.SchemaVersions']: [
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
          {userID: 123, handle: '@zoot', ['_0_version']: '00'},
          {userID: 456, handle: '@bonk', ['_0_version']: '00'},
        ],
      },
    },
  ];

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
    await dropReplicationSlot(upstream, replicationSlot(REPLICA_ID));
    await testDBs.drop(upstream, replica);
  }, 10000);

  for (const c of cases) {
    test(
      c.name,
      async () => {
        await initDB(upstream, c.upstreamSetup, c.upstreamPreState);
        await initDB(replica, c.replicaSetup, c.replicaPreState);

        await initSyncSchema(
          createSilentLogContext(),
          REPLICA_ID,
          replica,
          upstream,
          getConnectionURI(upstream),
        );

        await expectTables(upstream, c.upstreamPostState);
        await expectTables(replica, c.replicaPostState);

        // Check that internal replication tables have been created.
        await expectTables(replica, {
          ['_zero.TxLog']: [],
          ['_zero.ChangeLog']: [],
          ['_zero.InvalidationRegistry']: [],
          ['_zero.InvalidationIndex']: [],
        });

        // Subscriptions should have been dropped.
        const subs =
          await replica`SELECT subname FROM pg_subscription WHERE subname = 'zero_sync'`;
        expect(subs).toEqual([]);

        // Slot should still exist.
        const slots =
          await upstream`SELECT slot_name FROM pg_replication_slots WHERE slot_name = ${replicationSlot(
            REPLICA_ID,
          )}`.values();
        expect(slots[0]).toEqual([replicationSlot(REPLICA_ID)]);
      },
      10000,
    );
  }
});
