import {Database} from 'better-sqlite3';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {
  DbFile,
  expectTables,
  initDB as initLiteDB,
} from 'zero-cache/src/test/lite.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {
  dropReplicationSlot,
  getConnectionURI,
  initDB,
  testDBs,
} from '../../test/db.js';
import {initialSync, replicationSlot} from './initial-sync.js';
import {listTables} from './tables/list.js';
import {getPublicationInfo} from './tables/published.js';
import type {FilteredTableSpec, TableSpec} from './tables/specs.js';

const REPLICA_ID = 'initial_sync_test_id';

const ZERO_CLIENTS_SPEC: FilteredTableSpec = {
  columns: {
    clientGroupID: {
      characterMaximumLength: null,
      columnDefault: null,
      dataType: 'text',
      notNull: true,
    },
    clientID: {
      characterMaximumLength: null,
      columnDefault: null,
      dataType: 'text',
      notNull: true,
    },
    lastMutationID: {
      characterMaximumLength: null,
      columnDefault: null,
      dataType: 'int8',
      notNull: false,
    },
    userID: {
      characterMaximumLength: null,
      columnDefault: null,
      dataType: 'text',
      notNull: false,
    },
  },
  name: 'clients',
  primaryKey: ['clientGroupID', 'clientID'],
  schema: 'zero',
  filterConditions: [],
} as const;

const REPLICATED_ZERO_CLIENTS_SPEC: TableSpec = {
  columns: {
    clientGroupID: {
      characterMaximumLength: null,
      columnDefault: null,
      dataType: 'TEXT',
      notNull: false,
    },
    clientID: {
      characterMaximumLength: null,
      columnDefault: null,
      dataType: 'TEXT',
      notNull: false,
    },
    lastMutationID: {
      characterMaximumLength: null,
      columnDefault: null,
      dataType: 'INTEGER',
      notNull: false,
    },
    userID: {
      characterMaximumLength: null,
      columnDefault: null,
      dataType: 'TEXT',
      notNull: false,
    },
  },
  name: 'zero.clients',
  primaryKey: ['clientGroupID', 'clientID'],
  schema: '',
} as const;

describe('replicator/initial-sync', () => {
  type Case = {
    name: string;
    setupUpstreamQuery?: string;
    setupReplicaQuery?: string;
    published: Record<string, FilteredTableSpec>;
    upstream?: Record<string, object[]>;
    replicatedSchema: Record<string, TableSpec>;
    replicatedData: Record<string, object[]>;
    publications: string[];
  };

  const cases: Case[] = [
    {
      name: 'empty DB',
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
      },
      replicatedSchema: {
        ['zero.clients']: REPLICATED_ZERO_CLIENTS_SPEC,
      },
      replicatedData: {
        ['zero.clients']: [],
      },
      publications: ['zero_meta', 'zero_data'],
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
      replicatedSchema: {
        ['zero.clients']: REPLICATED_ZERO_CLIENTS_SPEC,
      },
      replicatedData: {
        ['zero.clients']: [],
      },
      publications: ['zero_meta', 'zero_data'],
    },
    {
      name: 'publication already setup',
      setupUpstreamQuery: `
      CREATE SCHEMA zero;
      CREATE TABLE zero.clients (
        "clientGroupID"  TEXT    NOT NULL,
        "clientID"       TEXT    NOT NULL,
        "lastMutationID" BIGINT,
        "userID"         TEXT,
        PRIMARY KEY("clientGroupID", "clientID")
      );
      CREATE PUBLICATION zero_meta FOR TABLES IN SCHEMA zero;
      CREATE PUBLICATION zero_data FOR TABLES IN SCHEMA zero, public;
      `,
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
      },
      replicatedSchema: {
        ['zero.clients']: REPLICATED_ZERO_CLIENTS_SPEC,
      },
      replicatedData: {
        ['zero.clients']: [],
      },
      publications: ['zero_meta', 'zero_data'],
    },
    {
      name: 'existing table, default publication',
      setupUpstreamQuery: `
        CREATE TABLE issues("issueID" INTEGER, "orgID" INTEGER, PRIMARY KEY ("orgID", "issueID"));
      `,
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
        ['public.issues']: {
          columns: {
            issueID: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'int4',
              notNull: true,
            },
            orgID: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'int4',
              notNull: true,
            },
          },
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
          schema: 'public',
          filterConditions: [],
        },
      },
      replicatedSchema: {
        ['zero.clients']: REPLICATED_ZERO_CLIENTS_SPEC,
        ['issues']: {
          columns: {
            issueID: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'INTEGER',
              notNull: false,
            },
            orgID: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'INTEGER',
              notNull: false,
            },
          },
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
          schema: '',
        },
      },
      upstream: {
        issues: [
          {issueID: 123, orgID: 456},
          {issueID: 321, orgID: 789},
        ],
      },
      replicatedData: {
        ['zero.clients']: [],
        issues: [
          {issueID: 123, orgID: 456, ['_0_version']: '00'},
          {issueID: 321, orgID: 789, ['_0_version']: '00'},
        ],
      },
      publications: ['zero_meta', 'zero_data'],
    },
    {
      name: 'existing partial publication',
      setupUpstreamQuery: `
        CREATE TABLE not_published("issueID" INTEGER, "orgID" INTEGER, PRIMARY KEY ("orgID", "issueID"));
        CREATE TABLE users("userID" INTEGER, password TEXT, handle TEXT, PRIMARY KEY ("userID"));
        CREATE PUBLICATION zero_custom FOR TABLE users ("userID", handle);
      `,
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
        ['public.users']: {
          columns: {
            userID: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'int4',
              notNull: true,
            },
            // Note: password is not published
            handle: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'text',
              notNull: false,
            },
          },
          name: 'users',
          primaryKey: ['userID'],
          schema: 'public',
          filterConditions: [],
        },
      },
      replicatedSchema: {
        ['zero.clients']: REPLICATED_ZERO_CLIENTS_SPEC,
        ['users']: {
          columns: {
            userID: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'INTEGER',
              notNull: false,
            },
            // Note: password is not published
            handle: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'TEXT',
              notNull: false,
            },
          },
          name: 'users',
          primaryKey: ['userID'],
          schema: '',
        },
      },
      upstream: {
        users: [
          {userID: 123, password: 'not-replicated', handle: '@zoot'},
          {userID: 456, password: 'super-secret', handle: '@bonk'},
        ],
      },
      replicatedData: {
        ['zero.clients']: [],
        users: [
          {userID: 123, handle: '@zoot', ['_0_version']: '00'},
          {userID: 456, handle: '@bonk', ['_0_version']: '00'},
        ],
      },
      publications: ['zero_meta', 'zero_custom'],
    },
    {
      name: 'existing partial filtered publication',
      setupUpstreamQuery: `
        CREATE TABLE not_published("issueID" INTEGER, "orgID" INTEGER, PRIMARY KEY ("orgID", "issueID"));
        CREATE TABLE users("userID" INTEGER, password TEXT, handle TEXT, PRIMARY KEY ("userID"));
        CREATE PUBLICATION zero_custom FOR TABLE users ("userID", handle) WHERE ("userID" % 2 = 0);
        CREATE PUBLICATION zero_custom2 FOR TABLE users ("userID", handle) WHERE ("userID" > 1000);
      `,
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
        ['public.users']: {
          columns: {
            userID: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'int4',
              notNull: true,
            },
            // Note: password is not published
            handle: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'text',
              notNull: false,
            },
          },
          name: 'users',
          primaryKey: ['userID'],
          schema: 'public',
          filterConditions: ['(("userID" % 2) = 0)', '("userID" > 1000)'],
        },
      },
      replicatedSchema: {
        ['zero.clients']: REPLICATED_ZERO_CLIENTS_SPEC,
        ['users']: {
          columns: {
            userID: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'INTEGER',
              notNull: false,
            },
            // Note: password is not published
            handle: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'TEXT',
              notNull: false,
            },
          },
          name: 'users',
          primaryKey: ['userID'],
          schema: '',
        },
      },
      upstream: {
        users: [
          {userID: 123, password: 'not-replicated', handle: '@zoot'},
          {userID: 456, password: 'super-secret', handle: '@bonk'},
          {userID: 1001, password: 'hide-me', handle: '@boom'},
        ],
      },
      replicatedData: {
        ['zero.clients']: [],
        users: [
          {userID: 456, handle: '@bonk', ['_0_version']: '00'},
          {userID: 1001, handle: '@boom', ['_0_version']: '00'},
        ],
      },
      publications: ['zero_meta', 'zero_custom', 'zero_custom2'],
    },
  ];

  let upstream: PostgresDB;
  let replicaFile: DbFile;
  let replica: Database;

  beforeEach(async () => {
    upstream = await testDBs.create('initial_sync_upstream');
    replicaFile = new DbFile('initial_sync_replica');
    replica = replicaFile.connect();
  });

  afterEach(async () => {
    await dropReplicationSlot(upstream, replicationSlot(REPLICA_ID));
    await testDBs.drop(upstream);
    await replicaFile.unlink();
  }, 10000);

  for (const c of cases) {
    test(`startInitialDataSynchronization: ${c.name}`, async () => {
      await initDB(upstream, c.setupUpstreamQuery, c.upstream);
      initLiteDB(replica, c.setupReplicaQuery);

      const lc = createSilentLogContext();
      await initialSync(
        lc,
        REPLICA_ID,
        replica,
        upstream,
        getConnectionURI(upstream, 'external'),
      );

      const {publications, tables} = await getPublicationInfo(upstream);
      expect(
        Object.fromEntries(
          tables.map(table => [`${table.schema}.${table.name}`, table]),
        ),
      ).toEqual(c.published);
      expect(new Set(publications.map(p => p.pubname))).toEqual(
        new Set(c.publications),
      );

      const synced = listTables(replica);
      expect(
        Object.fromEntries(synced.map(table => [table.name, table])),
      ).toMatchObject(c.replicatedSchema);
      const {pubNames} = replica
        .prepare(
          `SELECT publications as "pubNames" FROM "_zero.ReplicationState"`,
        )
        .get();
      expect(new Set(JSON.parse(pubNames))).toEqual(new Set(c.publications));

      expectTables(replica, c.replicatedData);

      const replicaState = replica
        .prepare('SELECT * FROM "_zero.ReplicationState"')
        .get();
      expect(replicaState).toMatchObject({
        publications: JSON.stringify(publications.map(p => p.pubname)),
        watermark: /[0-9A-F]+\/[0-9A-F]+/,
        nextStateVersion: /[0-9a-f]{2,}/,
      });

      // Check replica state against the upstream slot.
      const slots = await upstream`
        SELECT slot_name as "slotName", confirmed_flush_lsn as lsn 
          FROM pg_replication_slots WHERE slot_name = ${replicationSlot(
            REPLICA_ID,
          )}`;
      expect(slots[0]).toEqual({
        slotName: replicationSlot(REPLICA_ID),
        lsn: replicaState.watermark,
      });
    }, 10000);

    type InvalidUpstreamCase = {
      error: string;
      setupUpstreamQuery?: string;
      upstream?: Record<string, object[]>;
    };

    const invalidUpstreamCases: InvalidUpstreamCase[] = [
      {
        error: 'does not have a PRIMARY KEY',
        setupUpstreamQuery: `
        CREATE TABLE issues("issueID" INTEGER, "orgID" INTEGER);
      `,
      },
      {
        error: 'uses reserved column name "_0_version"',
        setupUpstreamQuery: `
        CREATE TABLE issues(
          "issueID" INTEGER PRIMARY KEY, 
          "orgID" INTEGER, 
          _0_version INTEGER);
      `,
      },
      {
        error: 'Schema "_zero" is reserved for internal use',
        setupUpstreamQuery: `
        CREATE SCHEMA _zero;
        CREATE TABLE _zero.is_not_allowed(
          "issueID" INTEGER PRIMARY KEY, 
          "orgID" INTEGER
        );
        CREATE PUBLICATION zero_foo FOR TABLES IN SCHEMA _zero;
        `,
      },
      {
        error: 'Only the default "public" schema is supported',
        setupUpstreamQuery: `
        CREATE SCHEMA unsupported;
        CREATE TABLE unsupported.issues ("issueID" INTEGER PRIMARY KEY, "orgID" INTEGER);
        CREATE PUBLICATION zero_foo FOR TABLES IN SCHEMA unsupported;
      `,
      },
      {
        error: 'Table "table/with/slashes" has invalid characters',
        setupUpstreamQuery: `
        CREATE TABLE "table/with/slashes" ("issueID" INTEGER PRIMARY KEY, "orgID" INTEGER);
      `,
      },
      {
        error: 'Table "table.with.dots" has invalid characters',
        setupUpstreamQuery: `
        CREATE TABLE "table.with.dots" ("issueID" INTEGER PRIMARY KEY, "orgID" INTEGER);
      `,
      },
      {
        error:
          'Column "column/with/slashes" in table "issues" has invalid characters',
        setupUpstreamQuery: `
        CREATE TABLE issues ("issueID" INTEGER PRIMARY KEY, "column/with/slashes" INTEGER);
      `,
      },
      {
        error:
          'Column "column.with.dots" in table "issues" has invalid characters',
        setupUpstreamQuery: `
        CREATE TABLE issues ("issueID" INTEGER PRIMARY KEY, "column.with.dots" INTEGER);
      `,
      },
    ];

    for (const c of invalidUpstreamCases) {
      test(`Invalid upstream: ${c.error}`, async () => {
        await initDB(upstream, c.setupUpstreamQuery, c.upstream);

        const result = await initialSync(
          createSilentLogContext(),
          REPLICA_ID,
          replica,
          upstream,
          getConnectionURI(upstream, 'external'),
        ).catch(e => e);

        expect(result).toBeInstanceOf(Error);
        expect(String(result)).toContain(c.error);
      });
    }
  }
});
