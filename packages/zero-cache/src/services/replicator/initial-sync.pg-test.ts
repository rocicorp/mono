import {
  afterAll,
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
} from '@jest/globals';
import type postgres from 'postgres';
import {TestDBs, expectTables, initDB} from '../../test/db.js';
import {createSilentLogContext} from '../../test/logger.js';
import {
  handoffPostgresReplication,
  replicationSlot,
  startPostgresReplication,
  waitForInitialDataSynchronization,
} from './initial-sync.js';
import {getPublicationInfo} from './tables/published.js';
import type {TableSpec} from './tables/specs.js';

const SUB = 'test_sync';
const REPLICA_ID = 'initial_sync_test_id';

const ZERO_CLIENTS_SPEC: TableSpec = {
  columns: {
    ['client_id']: {
      characterMaximumLength: null,
      columnDefault: null,
      dataType: 'text',
    },
    ['last_mutation_id']: {
      characterMaximumLength: null,
      columnDefault: null,
      dataType: 'int8',
    },
  },
  name: 'clients',
  primaryKey: ['client_id'],
  schema: 'zero',
} as const;

describe('replicator/initial-sync', () => {
  type Case = {
    name: string;
    setupUpstreamQuery?: string;
    setupReplicaQuery?: string;
    published: Record<string, TableSpec>;
    upstream?: Record<string, object[]>;
    replicated: Record<string, object[]>;
  };

  const cases: Case[] = [
    {
      name: 'empty DB',
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
      },
      replicated: {
        ['zero.clients']: [],
      },
    },
    {
      name: 'replication slot already exists',
      setupUpstreamQuery: `
        SELECT * FROM pg_create_logical_replication_slot('${replicationSlot(
          REPLICA_ID,
        )}', 'pgoutput');
      `,
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
      },
      replicated: {
        ['zero.clients']: [],
      },
    },
    {
      name: 'publication already setup',
      setupUpstreamQuery: `
      CREATE SCHEMA zero;
      CREATE TABLE zero.clients (
        client_id TEXT PRIMARY KEY,
        last_mutation_id BIGINT
      );
      CREATE PUBLICATION zero_metadata FOR TABLES IN SCHEMA zero;
      `,
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
      },
      replicated: {
        ['zero.clients']: [],
      },
    },
    {
      name: 'existing table, default publication',
      setupUpstreamQuery: `
        CREATE TABLE issues(issue_id INTEGER, org_id INTEGER, PRIMARY KEY (org_id, issue_id));
      `,
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
        ['public.issues']: {
          columns: {
            ['issue_id']: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'int4',
            },
            ['org_id']: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'int4',
            },
          },
          name: 'issues',
          primaryKey: ['org_id', 'issue_id'],
          schema: 'public',
        },
      },
      upstream: {
        issues: [
          {issueId: 123, orgId: 456},
          {issueId: 321, orgId: 789},
        ],
      },
      replicated: {
        ['zero.clients']: [],
        issues: [
          {issueId: 123, orgId: 456, ['_0Version']: '00'},
          {issueId: 321, orgId: 789, ['_0Version']: '00'},
        ],
      },
    },
    {
      name: 'existing partial publication',
      setupUpstreamQuery: `
        CREATE TABLE not_published(issue_id INTEGER, org_id INTEGER, PRIMARY KEY (org_id, issue_id));
        CREATE TABLE users(user_id INTEGER, password TEXT, handle TEXT, PRIMARY KEY (user_id));
        CREATE PUBLICATION zero_custom FOR TABLE users (user_id, handle);
      `,
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
        ['public.users']: {
          columns: {
            ['user_id']: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'int4',
            },
            // Note: password is not published
            ['handle']: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'text',
            },
          },
          name: 'users',
          primaryKey: ['user_id'],
          schema: 'public',
        },
      },
      upstream: {
        users: [
          {userId: 123, password: 'not-replicated', handle: '@zoot'},
          {userId: 456, password: 'super-secret', handle: '@bonk'},
        ],
      },
      replicated: {
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
    upstream = await testDBs.create('initial_sync_upstream');
    replica = await testDBs.create('initial_sync_replica');

    // This publication is used to query the tables that get created on the replica
    // using the same logic that's used for querying the published tables on upstream.
    await replica`CREATE PUBLICATION synced_tables FOR ALL TABLES`;
  });

  afterEach(async () => {
    // Technically done by the tested code, but this helps clean things up in the event of failures.
    await replica.begin(async tx => {
      const subs =
        await tx`SELECT subname FROM pg_subscription WHERE subname = ${SUB}`;
      if (subs.count > 0) {
        await tx.unsafe(`
        ALTER SUBSCRIPTION ${SUB} DISABLE;
        ALTER SUBSCRIPTION ${SUB} SET(slot_name=NONE);
        DROP SUBSCRIPTION IF EXISTS ${SUB};
      `);
      }
    });
    await upstream.begin(async tx => {
      const slots = await tx`
        SELECT slot_name FROM pg_replication_slots WHERE slot_name = ${replicationSlot(
          REPLICA_ID,
        )}`;
      if (slots.count > 0) {
        await tx`
          SELECT pg_drop_replication_slot(${replicationSlot(REPLICA_ID)});`;
      }
    });
    await testDBs.drop(upstream, replica);
  }, 10000);

  afterAll(async () => {
    await testDBs.end();
  });

  for (const c of cases) {
    test(`startInitialDataSynchronization: ${c.name}`, async () => {
      await initDB(upstream, c.setupUpstreamQuery, c.upstream);
      await initDB(replica, c.setupReplicaQuery);

      const lc = createSilentLogContext();
      await replica.begin(tx =>
        startPostgresReplication(
          lc,
          REPLICA_ID,
          tx,
          'postgres:///initial_sync_upstream',
          SUB,
        ),
      );

      const published = await getPublicationInfo(upstream, 'zero_');
      expect(published.tables).toEqual(c.published);

      const synced = await getPublicationInfo(replica, 'synced_tables');
      expect(synced.tables).toMatchObject(c.published);

      await replica.begin(tx =>
        waitForInitialDataSynchronization(
          lc,
          REPLICA_ID,
          tx,
          'postgres:///initial_sync_upstream',
          SUB,
        ),
      );

      await expectTables(replica, c.replicated);

      await replica.begin(tx =>
        handoffPostgresReplication(
          lc,
          REPLICA_ID,
          tx,
          'postgres:///initial_sync_upstream',
          SUB,
        ),
      );

      // Now verify that the Replication tables are initialized.
      await expectTables(replica, {
        ['zero.tx_log']: [],
        ['zero.change_log']: [],
        ['zero.invalidation_registry']: [],
        ['zero.invalidation_index']: [],
      });

      // Subscriptions should have been dropped.
      const subs =
        await replica`SELECT subname FROM pg_subscription WHERE subname = ${SUB}`;
      expect(subs).toEqual([]);

      // Slot should still exist.
      const slots =
        await upstream`SELECT slot_name FROM pg_replication_slots WHERE slot_name = ${replicationSlot(
          REPLICA_ID,
        )}`;
      expect(slots[0]).toEqual({slotName: replicationSlot(REPLICA_ID)});
    }, 10000);
  }
});
