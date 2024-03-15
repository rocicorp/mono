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
import {startPostgresReplication} from './initial-sync.js';
import {getPublishedTables} from './tables/published.js';
import type {TableSpec} from './tables/specs.js';

const SLOT = 'test_slot';

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
      dataType: 'bigint',
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
        SELECT * FROM pg_create_logical_replication_slot('test_slot', 'pgoutput');
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
              dataType: 'integer',
            },
            ['org_id']: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'integer',
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
              dataType: 'integer',
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
    // Theoretically, a simple DROP SUBSCRIPTION should take care of everything, but
    // this involves inter-Postgres communication to drop the corresponding slot on the
    // publisher DB which can results test flakiness.
    //
    // Things are more stable if the slot is released first, with the
    // subscription and slot explicitly deleted.
    await replica.unsafe(`
      ALTER SUBSCRIPTION zero_sync DISABLE;
      ALTER SUBSCRIPTION zero_sync SET(slot_name=NONE);
      DROP SUBSCRIPTION IF EXISTS zero_sync;
    `);
    await upstream.begin(async tx => {
      const slots = await tx`
        SELECT slot_name FROM pg_replication_slots WHERE slot_name = ${SLOT}`;
      if (slots.count > 0) {
        await tx`
          SELECT pg_drop_replication_slot(${SLOT});`;
      }
    });
    await testDBs.drop(upstream, replica);
  });

  afterAll(async () => {
    await testDBs.end();
  });

  for (const c of cases) {
    test(`startInitialDataSynchronization: ${c.name}`, async () => {
      await initDB(upstream, c.setupUpstreamQuery, c.upstream);
      await initDB(replica, c.setupReplicaQuery);

      await replica.begin(tx =>
        startPostgresReplication(
          createSilentLogContext(),
          tx,
          'postgres:///initial_sync_upstream',
          SLOT,
        ),
      );

      const published = await getPublishedTables(upstream, 'zero_');
      expect(published).toEqual(c.published);

      const slots =
        await upstream`SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = ${SLOT}`;
      expect(slots[0]).toEqual({count: '1'});

      const synced = await getPublishedTables(replica, 'synced_tables');
      expect(synced).toMatchObject(c.published);

      await expectTables(replica, c.replicated);
    });
  }
});
