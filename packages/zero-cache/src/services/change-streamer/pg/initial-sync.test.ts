import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.js';
import {Database} from '../../../../../zqlite/src/db.js';
import {listIndexes, listTables} from '../../../db/lite-tables.js';
import {
  dropReplicationSlot,
  getConnectionURI,
  initDB,
  testDBs,
} from '../../../test/db.js';
import {expectTables, initDB as initLiteDB} from '../../../test/lite.js';
import type {PostgresDB} from '../../../types/pg.js';
import type {
  FilteredTableSpec,
  IndexSpec,
  TableSpec,
} from '../../../types/specs.js';
import {initialSync, replicationSlot} from './initial-sync.js';
import {fromLexiVersion} from './lsn.js';
import {getPublicationInfo} from './schema/published.js';

const SHARD_ID = 'initial_sync_test_id';

const ZERO_SCHEMA_VERSIONS_SPEC: FilteredTableSpec = {
  columns: {
    minSupportedVersion: {
      characterMaximumLength: null,
      dataType: 'int4',
      dflt: null,
      notNull: false,
      pos: 1,
    },
    maxSupportedVersion: {
      characterMaximumLength: null,
      dataType: 'int4',
      dflt: null,
      notNull: false,
      pos: 2,
    },
    lock: {
      characterMaximumLength: null,
      dataType: 'bool',
      dflt: 'true',
      notNull: true,
      pos: 3,
    },
  },
  name: 'schemaVersions',
  primaryKey: ['lock'],
  publications: {['_zero_schema_versions']: {rowFilter: null}},
  schema: 'zero',
} as const;

const ZERO_CLIENTS_SPEC: FilteredTableSpec = {
  columns: {
    shardID: {
      pos: 1,
      characterMaximumLength: null,
      dataType: 'text',
      notNull: true,
      dflt: null,
    },
    clientGroupID: {
      pos: 2,
      characterMaximumLength: null,
      dataType: 'text',
      notNull: true,
      dflt: null,
    },
    clientID: {
      pos: 3,
      characterMaximumLength: null,
      dataType: 'text',
      notNull: true,
      dflt: null,
    },
    lastMutationID: {
      pos: 4,
      characterMaximumLength: null,
      dataType: 'int8',
      notNull: true,
      dflt: null,
    },
    userID: {
      pos: 5,
      characterMaximumLength: null,
      dataType: 'text',
      notNull: false,
      dflt: null,
    },
  },
  name: 'clients',
  primaryKey: ['shardID', 'clientGroupID', 'clientID'],
  schema: 'zero',
  publications: {
    ['_zero_initial_sync_test_id_clients']: {
      rowFilter: `("shardID" = 'initial_sync_test_id'::text)`,
    },
  },
} as const;

const REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC: TableSpec = {
  columns: {
    minSupportedVersion: {
      characterMaximumLength: null,
      dataType: 'int4',
      dflt: null,
      notNull: false,
      pos: 1,
    },
    maxSupportedVersion: {
      characterMaximumLength: null,
      dataType: 'int4',
      dflt: null,
      notNull: false,
      pos: 2,
    },
    lock: {
      characterMaximumLength: null,
      dataType: 'bool',
      dflt: null,
      notNull: true,
      pos: 3,
    },
  },
  name: 'zero.schemaVersions',
  primaryKey: ['lock'],
  schema: '',
} as const;

const REPLICATED_ZERO_CLIENTS_SPEC: TableSpec = {
  columns: {
    shardID: {
      pos: 1,
      characterMaximumLength: null,
      dataType: 'TEXT',
      notNull: true,
      dflt: null,
    },
    clientGroupID: {
      pos: 2,
      characterMaximumLength: null,
      dataType: 'TEXT',
      notNull: true,
      dflt: null,
    },
    clientID: {
      pos: 3,
      characterMaximumLength: null,
      dataType: 'TEXT',
      notNull: true,
      dflt: null,
    },
    lastMutationID: {
      pos: 4,
      characterMaximumLength: null,
      dataType: 'int8',
      notNull: true,
      dflt: null,
    },
    userID: {
      pos: 5,
      characterMaximumLength: null,
      dataType: 'TEXT',
      notNull: false,
      dflt: null,
    },
  },
  name: 'zero.clients',
  primaryKey: ['shardID', 'clientGroupID', 'clientID'],
  schema: '',
} as const;

describe('replicator/initial-sync', () => {
  type Case = {
    name: string;
    setupUpstreamQuery?: string;
    requestedPublications?: string[];
    setupReplicaQuery?: string;
    published: Record<string, FilteredTableSpec>;
    upstream?: Record<string, object[]>;
    replicatedSchema: Record<string, TableSpec>;
    replicatedIndices?: IndexSpec[];
    replicatedData: Record<string, object[]>;
    resultingPublications: string[];
  };

  const cases: Case[] = [
    {
      name: 'empty DB',
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: ZERO_SCHEMA_VERSIONS_SPEC,
      },
      replicatedSchema: {
        ['zero.clients']: REPLICATED_ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
      },
      replicatedData: {
        ['zero.clients']: [],
        ['zero.schemaVersions']: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: '00',
          },
        ],
      },
      resultingPublications: [
        '_zero_initial_sync_test_id_clients',
        '_zero_schema_versions',
        'zero_public',
      ],
    },
    {
      name: 'replication slot already exists',
      setupUpstreamQuery: `
        SELECT * FROM pg_create_logical_replication_slot('${replicationSlot(
          SHARD_ID,
        )}', 'pgoutput');
      `,
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: ZERO_SCHEMA_VERSIONS_SPEC,
      },
      replicatedSchema: {
        ['zero.clients']: REPLICATED_ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
      },
      replicatedData: {
        ['zero.clients']: [],
        ['zero.schemaVersions']: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: '00',
          },
        ],
      },
      resultingPublications: [
        '_zero_initial_sync_test_id_clients',
        '_zero_schema_versions',
        'zero_public',
      ],
    },
    {
      name: 'existing table, default publication',
      setupUpstreamQuery: `
        CREATE TABLE issues(
          "issueID" INTEGER,
          "orgID" INTEGER,
          "isAdmin" BOOLEAN,
          "bigint" BIGINT,
          "time" TIMESTAMPTZ,
          "bytes" BYTEA,
          "intArray" INTEGER[],
          "json" JSON,
          "jsonb" JSONB,
          PRIMARY KEY ("orgID", "issueID")
        );
      `,
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: ZERO_SCHEMA_VERSIONS_SPEC,
        ['public.issues']: {
          columns: {
            issueID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
              dflt: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
              dflt: null,
            },
            isAdmin: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'bool',
              notNull: false,
              dflt: null,
            },
            bigint: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'int8',
              notNull: false,
              dflt: null,
            },
            time: {
              pos: 5,
              characterMaximumLength: null,
              dataType: 'timestamptz',
              notNull: false,
              dflt: null,
            },
            bytes: {
              pos: 6,
              characterMaximumLength: null,
              dataType: 'bytea',
              notNull: false,
              dflt: null,
            },
            intArray: {
              pos: 7,
              characterMaximumLength: null,
              dataType: 'int4[]',
              notNull: false,
              dflt: null,
            },
            json: {
              pos: 8,
              characterMaximumLength: null,
              dataType: 'json',
              notNull: false,
              dflt: null,
            },
            jsonb: {
              pos: 9,
              characterMaximumLength: null,
              dataType: 'jsonb',
              notNull: false,
              dflt: null,
            },
          },
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
          schema: 'public',
          publications: {['zero_public']: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        ['zero.clients']: REPLICATED_ZERO_CLIENTS_SPEC,
        ['issues']: {
          columns: {
            issueID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
              dflt: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
              dflt: null,
            },
            isAdmin: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'bool',
              notNull: false,
              dflt: null,
            },
            bigint: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'int8',
              notNull: false,
              dflt: null,
            },
            time: {
              pos: 5,
              characterMaximumLength: null,
              dataType: 'timestamptz',
              notNull: false,
              dflt: null,
            },
            bytes: {
              pos: 6,
              characterMaximumLength: null,
              dataType: 'bytea',
              notNull: false,
              dflt: null,
            },
            intArray: {
              pos: 7,
              characterMaximumLength: null,
              dataType: 'int4[]',
              notNull: false,
              dflt: null,
            },
            json: {
              pos: 8,
              characterMaximumLength: null,
              dataType: 'json',
              notNull: false,
              dflt: null,
            },
            jsonb: {
              pos: 9,
              characterMaximumLength: null,
              dataType: 'jsonb',
              notNull: false,
              dflt: null,
            },
            ['_0_version']: {
              pos: 10,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: true,
              dflt: null,
            },
          },
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
          schema: '',
        },
      },
      upstream: {
        issues: [
          {
            issueID: 123,
            orgID: 456,
            isAdmin: true,
            bigint: 99999999999999999n,
            time: null,
            bytes: null,
            intArray: null,
            json: {foo: 'bar'},
            jsonb: {bar: 'baz'},
          },
          {
            issueID: 321,
            orgID: 789,
            isAdmin: null,
            bigint: null,
            time: '2019-01-12T00:30:35.381101032Z',
            bytes: null,
            intArray: null,
            json: [1, 2, 3],
            jsonb: [{boo: 123}],
          },
          {
            issueID: 456,
            orgID: 789,
            isAdmin: false,
            bigint: null,
            time: null,
            bytes: Buffer.from('hello'),
            intArray: [1, 2],
            json: null,
            jsonb: null,
          },
        ],
      },
      replicatedData: {
        ['zero.clients']: [],
        issues: [
          {
            issueID: 123n,
            orgID: 456n,
            isAdmin: 1n,
            bigint: 99999999999999999n,
            time: null,
            bytes: null,
            intArray: null,
            json: '{"foo":"bar"}',
            jsonb: '{"bar":"baz"}',
            ['_0_version']: '00',
          },
          {
            issueID: 321n,
            orgID: 789n,
            isAdmin: null,
            bigint: null,
            time: 1547253035381101n,
            bytes: null,
            intArray: null,
            json: '[1,2,3]',
            jsonb: '[{"boo":123}]',
            ['_0_version']: '00',
          },
          {
            issueID: 456n,
            orgID: 789n,
            isAdmin: 0n,
            bigint: null,
            time: null,
            bytes: Buffer.from('hello'),
            intArray: '[1,2]',
            json: null,
            jsonb: null,
            ['_0_version']: '00',
          },
        ],
        ['zero.schemaVersions']: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: '00',
          },
        ],
      },
      resultingPublications: [
        '_zero_initial_sync_test_id_clients',
        '_zero_schema_versions',
        'zero_public',
      ],
    },
    {
      name: 'existing partial publication',
      setupUpstreamQuery: `
        CREATE TABLE not_published("issueID" INTEGER, "orgID" INTEGER, PRIMARY KEY ("orgID", "issueID"));
        CREATE TABLE users("userID" INTEGER, password TEXT, handle TEXT, PRIMARY KEY ("userID"));
        CREATE PUBLICATION zero_custom FOR TABLE users ("userID", handle);
      `,
      requestedPublications: ['zero_custom'],
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: ZERO_SCHEMA_VERSIONS_SPEC,
        ['public.users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
              dflt: null,
            },
            // Note: password is not published
            handle: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'text',
              notNull: false,
              dflt: null,
            },
          },
          name: 'users',
          primaryKey: ['userID'],
          schema: 'public',
          publications: {['zero_custom']: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        ['zero.clients']: REPLICATED_ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
        ['users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
              dflt: null,
            },
            // Note: password is not published
            handle: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              dflt: null,
            },
            ['_0_version']: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: true,
              dflt: null,
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
          {userID: 123n, handle: '@zoot', ['_0_version']: '00'},
          {userID: 456n, handle: '@bonk', ['_0_version']: '00'},
        ],
        ['zero.schemaVersions']: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: '00',
          },
        ],
      },
      resultingPublications: [
        '_zero_initial_sync_test_id_clients',
        '_zero_schema_versions',
        'zero_custom',
      ],
    },
    {
      name: 'existing partial filtered publication',
      setupUpstreamQuery: `
        CREATE TABLE not_published("issueID" INTEGER, "orgID" INTEGER, PRIMARY KEY ("orgID", "issueID"));
        CREATE TABLE users("userID" INTEGER, password TEXT, handle TEXT, PRIMARY KEY ("userID"));
        CREATE PUBLICATION zero_custom FOR TABLE users ("userID", handle) WHERE ("userID" % 2 = 0);
        CREATE PUBLICATION zero_custom2 FOR TABLE users ("userID", handle) WHERE ("userID" > 1000);
      `,
      requestedPublications: ['zero_custom', 'zero_custom2'],
      published: {
        ['zero.clients']: ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: ZERO_SCHEMA_VERSIONS_SPEC,
        ['public.users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
              dflt: null,
            },
            // Note: password is not published
            handle: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'text',
              notNull: false,
              dflt: null,
            },
          },
          name: 'users',
          primaryKey: ['userID'],
          schema: 'public',
          publications: {
            ['zero_custom']: {rowFilter: '(("userID" % 2) = 0)'},
            ['zero_custom2']: {rowFilter: '("userID" > 1000)'},
          },
        },
      },
      replicatedSchema: {
        ['zero.clients']: REPLICATED_ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
        ['users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
              dflt: null,
            },
            // Note: password is not published
            handle: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              dflt: null,
            },
            ['_0_version']: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: true,
              dflt: null,
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
          {userID: 456n, handle: '@bonk', ['_0_version']: '00'},
          {userID: 1001n, handle: '@boom', ['_0_version']: '00'},
        ],
        ['zero.schemaVersions']: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: '00',
          },
        ],
      },
      resultingPublications: [
        '_zero_initial_sync_test_id_clients',
        '_zero_schema_versions',
        'zero_custom',
        'zero_custom2',
      ],
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
        ['zero.schemaVersions']: ZERO_SCHEMA_VERSIONS_SPEC,
        ['public.issues']: {
          columns: {
            issueID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
              dflt: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
              dflt: null,
            },
            other: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'text',
              notNull: false,
              dflt: null,
            },
            isAdmin: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'bool',
              notNull: false,
              dflt: null,
            },
          },
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
          schema: 'public',
          publications: {['zero_public']: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        ['zero.clients']: REPLICATED_ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
        ['issues']: {
          columns: {
            issueID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
              dflt: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: true,
              dflt: null,
            },
            other: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              dflt: null,
            },
            isAdmin: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'bool',
              notNull: false,
              dflt: null,
            },
            ['_0_version']: {
              pos: 5,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: true,
              dflt: null,
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
        ['zero.schemaVersions']: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: '00',
          },
        ],
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
      resultingPublications: [
        '_zero_initial_sync_test_id_clients',
        '_zero_schema_versions',
        'zero_public',
      ],
    },
  ];

  let upstream: PostgresDB;
  let replica: Database;

  beforeEach(async () => {
    upstream = await testDBs.create('initial_sync_upstream');
    replica = new Database(createSilentLogContext(), ':memory:');
  });

  afterEach(async () => {
    await dropReplicationSlot(upstream, replicationSlot(SHARD_ID));
    await testDBs.drop(upstream);
  });

  for (const c of cases) {
    test(`startInitialDataSynchronization: ${c.name}`, async () => {
      await initDB(upstream, c.setupUpstreamQuery, c.upstream);
      initLiteDB(replica, c.setupReplicaQuery);

      const lc = createSilentLogContext();
      await initialSync(
        lc,
        {id: SHARD_ID, publications: c.requestedPublications ?? []},
        replica,
        getConnectionURI(upstream),
      );

      const {publications, tables} = await getPublicationInfo(upstream);
      expect(
        Object.fromEntries(
          tables.map(table => [`${table.schema}.${table.name}`, table]),
        ),
      ).toEqual(c.published);
      expect(new Set(publications.map(p => p.pubname))).toEqual(
        new Set(c.resultingPublications),
      );

      const synced = listTables(replica);
      expect(
        Object.fromEntries(synced.map(table => [table.name, table])),
      ).toMatchObject(c.replicatedSchema);
      const {pubs} = replica
        .prepare(`SELECT publications as pubs FROM "_zero.ReplicationConfig"`)
        .get<{pubs: string}>();
      expect(new Set(JSON.parse(pubs))).toEqual(
        new Set(c.resultingPublications),
      );

      const syncedIndices = listIndexes(replica);
      expect(syncedIndices).toEqual(c.replicatedIndices ?? []);

      expectTables(replica, c.replicatedData, 'bigint');

      const replicaState = replica
        .prepare('SELECT * FROM "_zero.ReplicationState"')
        .get<{
          watermark: string;
          stateVersion: string;
          nextStateVersion: string;
          lock: number;
        }>();
      expect(replicaState).toMatchObject({
        watermark: /[0-9a-f]{2,}/,
        stateVersion: '00',
      });
      expectTables(replica, {['_zero.ChangeLog']: []});

      // Check replica state against the upstream slot.
      const slots = await upstream`
        SELECT slot_name as "slotName", confirmed_flush_lsn as lsn 
          FROM pg_replication_slots WHERE slot_name = ${replicationSlot(
            SHARD_ID,
          )}`;
      expect(slots[0]).toEqual({
        slotName: replicationSlot(SHARD_ID),
        lsn: fromLexiVersion(replicaState.watermark),
      });
    });
  }
});
