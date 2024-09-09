import {Database} from 'zqlite/src/db.js';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {expectTables, initDB as initLiteDB} from 'zero-cache/src/test/lite.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {
  dropReplicationSlot,
  getConnectionURI,
  initDB,
  testDBs,
} from '../../test/db.js';
import {initialSync, replicationSlot} from './initial-sync.js';
import {listIndices, listTables} from './tables/list.js';
import {getPublicationInfo} from './tables/published.js';
import type {FilteredTableSpec, IndexSpec, TableSpec} from './tables/specs.js';

const REPLICA_ID = 'initial_sync_test_id';

const ZERO_CLIENTS_SPEC: FilteredTableSpec = {
  columns: {
    clientGroupID: {
      characterMaximumLength: null,
      dataType: 'text',
      notNull: true,
    },
    clientID: {
      characterMaximumLength: null,
      dataType: 'text',
      notNull: true,
    },
    lastMutationID: {
      characterMaximumLength: null,
      dataType: 'int8',
      notNull: false,
    },
    userID: {
      characterMaximumLength: null,
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
      dataType: 'TEXT',
      notNull: false,
    },
    clientID: {
      characterMaximumLength: null,
      dataType: 'TEXT',
      notNull: false,
    },
    lastMutationID: {
      characterMaximumLength: null,
      dataType: 'INTEGER',
      notNull: false,
    },
    userID: {
      characterMaximumLength: null,
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
    replicatedIndices?: IndexSpec[];
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
        CREATE TABLE issues(
          "issueID" INTEGER,
          "orgID" INTEGER,
          "isAdmin" BOOLEAN,
          PRIMARY KEY ("orgID", "issueID")
        );
      `,
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
        ['public.issues']: {
          columns: {
            issueID: {
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
            },
            orgID: {
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
            },
            isAdmin: {
              characterMaximumLength: null,
              dataType: 'bool',
              notNull: false,
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
              dataType: 'INTEGER',
              notNull: false,
            },
            orgID: {
              characterMaximumLength: null,
              dataType: 'INTEGER',
              notNull: false,
            },
            isAdmin: {
              characterMaximumLength: null,
              dataType: 'BOOL',
              notNull: false,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: true,
            },
          },
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
          schema: '',
        },
      },
      upstream: {
        issues: [
          {issueID: 123, orgID: 456, isAdmin: true},
          {issueID: 321, orgID: 789, isAdmin: null},
          {issueID: 456, orgID: 789, isAdmin: false},
        ],
      },
      replicatedData: {
        ['zero.clients']: [],
        issues: [
          {issueID: 123, orgID: 456, isAdmin: 1, ['_0_version']: '00'},
          {issueID: 321, orgID: 789, isAdmin: null, ['_0_version']: '00'},
          {issueID: 456, orgID: 789, isAdmin: 0, ['_0_version']: '00'},
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
              dataType: 'int4',
              notNull: true,
            },
            // Note: password is not published
            handle: {
              characterMaximumLength: null,
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
              dataType: 'INTEGER',
              notNull: false,
            },
            // Note: password is not published
            handle: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: true,
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
              dataType: 'int4',
              notNull: true,
            },
            // Note: password is not published
            handle: {
              characterMaximumLength: null,
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
              dataType: 'INTEGER',
              notNull: false,
            },
            // Note: password is not published
            handle: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: true,
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
    {
      name: 'replicates indices',
      setupUpstreamQuery: `
        CREATE TABLE issues(
          "issueID" INTEGER,
          "orgID" INTEGER,
          "other" TEXT,
          "isAdmin" BOOLEAN,
          PRIMARY KEY ("orgID", "issueID")
        );
        CREATE INDEX ON issues ("orgID", "other");
      `,
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
        ['public.issues']: {
          columns: {
            issueID: {
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
            },
            orgID: {
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
            },
            other: {
              characterMaximumLength: null,
              dataType: 'text',
              notNull: false,
            },
            isAdmin: {
              characterMaximumLength: null,
              dataType: 'bool',
              notNull: false,
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
              dataType: 'INTEGER',
              notNull: false,
            },
            orgID: {
              characterMaximumLength: null,
              dataType: 'INTEGER',
              notNull: false,
            },
            isAdmin: {
              characterMaximumLength: null,
              dataType: 'BOOL',
              notNull: false,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: true,
            },
          },
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
          schema: '',
        },
      },
      upstream: {},
      replicatedData: {
        ['zero.clients']: [],
        issues: [],
      },
      replicatedIndices: [
        {
          columns: ['orgID', 'other'],
          name: 'issues_orgID_other_idx',
          schemaName: '',
          tableName: 'issues',
          unique: false,
        },
      ],
      publications: ['zero_meta', 'zero_data'],
    },
  ];

  let upstream: PostgresDB;
  let replica: Database;

  beforeEach(async () => {
    upstream = await testDBs.create('initial_sync_upstream');
    replica = new Database(createSilentLogContext(), ':memory:');
  });

  afterEach(async () => {
    await dropReplicationSlot(upstream, replicationSlot(REPLICA_ID));
    await testDBs.drop(upstream);
  });

  for (const c of cases) {
    test(`startInitialDataSynchronization: ${c.name}`, async () => {
      await initDB(upstream, c.setupUpstreamQuery, c.upstream);
      initLiteDB(replica, c.setupReplicaQuery);

      const lc = createSilentLogContext();
      await initialSync(lc, REPLICA_ID, replica, getConnectionURI(upstream));

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
      const {pubs} = replica
        .prepare(`SELECT publications as pubs FROM "_zero.ReplicationConfig"`)
        .get<{pubs: string}>();
      expect(new Set(JSON.parse(pubs))).toEqual(new Set(c.publications));

      const syncedIndices = listIndices(replica);
      expect(syncedIndices).toEqual(c.replicatedIndices ?? []);

      expectTables(replica, c.replicatedData);

      const replicaState = replica
        .prepare('SELECT * FROM "_zero.ReplicationState"')
        .get<{
          watermark: string;
          stateVersion: string;
          nextStateVersion: string;
          lock: number;
        }>();
      expect(replicaState).toMatchObject({
        watermark: /[0-9A-F]+\/[0-9A-F]+/,
        stateVersion: '00',
        nextStateVersion: /[0-9a-f]{2,}/,
      });
      expectTables(replica, {['_zero.ChangeLog']: []});

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
    });
  }
});
