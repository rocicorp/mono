import {afterAll, afterEach, beforeEach, describe, test} from '@jest/globals';
import type postgres from 'postgres';
import {sleep} from 'shared/src/sleep.js';
import {TestDBs, expectTables, initDB} from '../../../test/db.js';
import {createSilentLogContext} from '../../../test/logger.js';
import {initSyncSchema} from './sync-schema.js';

describe('schema/sync', () => {
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
            version: 2,
            maxVersion: 2,
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
            version: 2,
            maxVersion: 2,
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
    // Theoretically, a simple DROP SUBSCRIPTION should take care of everything, but
    // this involves inter-Postgres communication to drop the corresponding slot on the
    // publisher DB which can results test flakiness.
    //
    // Things are more stable if the slot is released first, with the
    // subscription and slot explicitly deleted.
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
  });

  afterAll(async () => {
    await testDBs.end();
  });

  for (const c of cases) {
    test(c.name, async () => {
      await initDB(upstream, c.upstreamSetup, c.upstreamPreState);
      await initDB(replica, c.replicaSetup, c.replicaPreState);
      await initSyncSchema(
        createSilentLogContext(),
        replica,
        `postgres:///${upstream.options.database}`,
      );

      // Poll the replica to wait for tables to sync.
      // Note that this will eventually be moved into a migration step.
      for (let i = 0; i < 100; i++) {
        const syncingTables =
          await replica`SELECT * FROM pg_subscription_rel WHERE srsubstate != 'r'`;
        if (syncingTables.count > 0) {
          console.debug(`Waiting for ${syncingTables.count} tables to sync`);
          await sleep(50);
        }
      }

      await expectTables(upstream, c.upstreamPostState);
      await expectTables(replica, c.replicaPostState);
    });
  }
});
