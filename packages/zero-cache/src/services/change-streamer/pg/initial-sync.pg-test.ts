import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.js';
import {Database} from '../../../../../zqlite/src/db.js';
import {listIndexes, listTables} from '../../../db/lite-tables.js';
import type {
  LiteIndexSpec,
  LiteTableSpec,
  PublishedTableSpec,
} from '../../../db/specs.js';
import {getConnectionURI, initDB, testDBs} from '../../../test/db.js';
import {expectTables, initDB as initLiteDB} from '../../../test/lite.js';
import type {PostgresDB} from '../../../types/pg.js';
import {initialSync, replicationSlot} from './initial-sync.js';
import {fromLexiVersion} from './lsn.js';
import {initShardSchema} from './schema/init.js';
import {getPublicationInfo} from './schema/published.js';
import {UnsupportedTableSchemaError} from './schema/validation.js';

const SHARD_ID = 'initial_sync_test_id';

const ZERO_SCHEMA_VERSIONS_SPEC: PublishedTableSpec = {
  columns: {
    minSupportedVersion: {
      characterMaximumLength: null,
      dataType: 'int4',
      typeOID: 23,
      dflt: null,
      notNull: false,
      pos: 1,
    },
    maxSupportedVersion: {
      characterMaximumLength: null,
      dataType: 'int4',
      typeOID: 23,
      dflt: null,
      notNull: false,
      pos: 2,
    },
    lock: {
      characterMaximumLength: null,
      dataType: 'bool',
      typeOID: 16,
      dflt: 'true',
      notNull: true,
      pos: 3,
    },
  },
  oid: expect.any(Number),
  name: 'schemaVersions',
  primaryKey: ['lock'],
  publications: {[`_zero_metadata_${SHARD_ID}`]: {rowFilter: null}},
  schema: 'zero',
} as const;

const ZERO_CLIENTS_SPEC: PublishedTableSpec = {
  columns: {
    clientGroupID: {
      pos: 1,
      characterMaximumLength: null,
      dataType: 'text',
      typeOID: 25,
      notNull: true,
      dflt: null,
    },
    clientID: {
      pos: 2,
      characterMaximumLength: null,
      dataType: 'text',
      typeOID: 25,
      notNull: true,
      dflt: null,
    },
    lastMutationID: {
      pos: 3,
      characterMaximumLength: null,
      dataType: 'int8',
      typeOID: 20,
      notNull: true,
      dflt: null,
    },
    userID: {
      pos: 4,
      characterMaximumLength: null,
      dataType: 'text',
      typeOID: 25,
      notNull: false,
      dflt: null,
    },
  },
  oid: expect.any(Number),
  name: 'clients',
  primaryKey: ['clientGroupID', 'clientID'],
  schema: `zero_${SHARD_ID}`,
  publications: {[`_zero_metadata_${SHARD_ID}`]: {rowFilter: null}},
} as const;

const REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC: LiteTableSpec = {
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
      notNull: false,
      pos: 3,
    },
  },
  name: 'zero.schemaVersions',
  primaryKey: ['lock'],
} as const;

const REPLICATED_ZERO_CLIENTS_SPEC: LiteTableSpec = {
  columns: {
    clientGroupID: {
      pos: 1,
      characterMaximumLength: null,
      dataType: 'TEXT',
      notNull: false,
      dflt: null,
    },
    clientID: {
      pos: 2,
      characterMaximumLength: null,
      dataType: 'TEXT',
      notNull: false,
      dflt: null,
    },
    lastMutationID: {
      pos: 3,
      characterMaximumLength: null,
      dataType: 'int8',
      notNull: false,
      dflt: null,
    },
    userID: {
      pos: 4,
      characterMaximumLength: null,
      dataType: 'TEXT',
      notNull: false,
      dflt: null,
    },
  },
  name: `zero_${SHARD_ID}.clients`,
  primaryKey: ['clientGroupID', 'clientID'],
} as const;

describe('replicator/initial-sync', () => {
  type Case = {
    name: string;
    setupUpstreamQuery?: string;
    requestedPublications?: string[];
    setupReplicaQuery?: string;
    published: Record<string, PublishedTableSpec>;
    upstream?: Record<string, object[]>;
    replicatedSchema: Record<string, LiteTableSpec>;
    replicatedIndices?: LiteIndexSpec[];
    replicatedData: Record<string, object[]>;
    resultingPublications: string[];
  };

  const cases: Case[] = [
    {
      name: 'empty DB',
      published: {
        [`zero_${SHARD_ID}.clients`]: ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: ZERO_SCHEMA_VERSIONS_SPEC,
      },
      replicatedSchema: {
        [`zero_${SHARD_ID}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
      },
      replicatedData: {
        [`zero_${SHARD_ID}.clients`]: [],
        ['zero.schemaVersions']: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: '00',
          },
        ],
      },
      resultingPublications: [`_zero_metadata_${SHARD_ID}`, 'zero_public'],
    },
    {
      name: 'replication slot already exists',
      setupUpstreamQuery: `
        SELECT * FROM pg_create_logical_replication_slot('${replicationSlot(
          SHARD_ID,
        )}', 'pgoutput');
      `,
      published: {
        [`zero_${SHARD_ID}.clients`]: ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: ZERO_SCHEMA_VERSIONS_SPEC,
      },
      replicatedSchema: {
        [`zero_${SHARD_ID}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
      },
      replicatedData: {
        [`zero_${SHARD_ID}.clients`]: [],
        ['zero.schemaVersions']: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: '00',
          },
        ],
      },
      resultingPublications: [`_zero_metadata_${SHARD_ID}`, 'zero_public'],
    },
    {
      name: 'existing table, default publication',
      setupUpstreamQuery: `
        CREATE TABLE issues(
          "issueID" INTEGER,
          "orgID" INTEGER,
          "isAdmin" BOOLEAN,
          "bigint" BIGINT,
          "timestamp" TIMESTAMPTZ,
          "bytes" BYTEA,
          "intArray" INTEGER[],
          "json" JSON,
          "jsonb" JSONB,
          "date" DATE,
          "time" TIME,
          "serial" SERIAL,
          PRIMARY KEY ("orgID", "issueID")
        );
      `,
      published: {
        [`zero_${SHARD_ID}.clients`]: ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: ZERO_SCHEMA_VERSIONS_SPEC,
        ['public.issues']: {
          columns: {
            issueID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
            },
            isAdmin: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'bool',
              typeOID: 16,
              notNull: false,
              dflt: null,
            },
            bigint: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'int8',
              typeOID: 20,
              notNull: false,
              dflt: null,
            },
            timestamp: {
              pos: 5,
              characterMaximumLength: null,
              dataType: 'timestamptz',
              typeOID: 1184,
              notNull: false,
              dflt: null,
            },
            bytes: {
              pos: 6,
              characterMaximumLength: null,
              dataType: 'bytea',
              typeOID: 17,
              notNull: false,
              dflt: null,
            },
            intArray: {
              pos: 7,
              characterMaximumLength: null,
              dataType: 'int4[]',
              typeOID: 1007,
              notNull: false,
              dflt: null,
            },
            json: {
              pos: 8,
              characterMaximumLength: null,
              dataType: 'json',
              typeOID: 114,
              notNull: false,
              dflt: null,
            },
            jsonb: {
              pos: 9,
              characterMaximumLength: null,
              dataType: 'jsonb',
              typeOID: 3802,
              notNull: false,
              dflt: null,
            },
            date: {
              pos: 10,
              characterMaximumLength: null,
              dataType: 'date',
              typeOID: 1082,
              notNull: false,
              dflt: null,
            },
            time: {
              pos: 11,
              characterMaximumLength: null,
              dataType: 'time',
              typeOID: 1083,
              notNull: false,
              dflt: null,
            },
            serial: {
              pos: 12,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              dflt: "nextval('issues_serial_seq'::regclass)",
              notNull: true,
            },
          },
          oid: expect.any(Number),
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
          schema: 'public',
          publications: {['zero_public']: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        [`zero_${SHARD_ID}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        ['issues']: {
          columns: {
            issueID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: false,
              dflt: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: false,
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
            timestamp: {
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
            date: {
              pos: 10,
              characterMaximumLength: null,
              dataType: 'date',
              notNull: false,
              dflt: null,
            },
            time: {
              pos: 11,
              characterMaximumLength: null,
              dataType: 'time',
              notNull: false,
              dflt: null,
            },
            serial: {
              pos: 12,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: false,
              dflt: null,
            },
            ['_0_version']: {
              pos: 13,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              dflt: null,
            },
          },
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
        },
      },
      upstream: {
        issues: [
          {
            issueID: 123,
            orgID: 456,
            isAdmin: true,
            bigint: 99999999999999999n,
            timestamp: null,
            bytes: null,
            intArray: null,
            json: {foo: 'bar'},
            jsonb: {bar: 'baz'},
            date: null,
            time: null,
          },
          {
            issueID: 321,
            orgID: 789,
            isAdmin: null,
            bigint: null,
            timestamp: '2019-01-12T00:30:35.381101032Z',
            bytes: null,
            intArray: null,
            json: [1, 2, 3],
            jsonb: [{boo: 123}],
            date: null,
            time: null,
          },
          {
            issueID: 456,
            orgID: 789,
            isAdmin: false,
            bigint: null,
            timestamp: null,
            bytes: Buffer.from('hello'),
            intArray: [1, 2],
            json: null,
            jsonb: null,
            date: Date.UTC(2003, 3, 23),
            time: '09:10:11.123456789',
          },
        ],
      },
      replicatedData: {
        [`zero_${SHARD_ID}.clients`]: [],
        issues: [
          {
            issueID: 123n,
            orgID: 456n,
            isAdmin: 1n,
            bigint: 99999999999999999n,
            timestamp: null,
            bytes: null,
            intArray: null,
            json: '{"foo":"bar"}',
            jsonb: '{"bar":"baz"}',
            date: null,
            time: null,
            serial: 1n,
            ['_0_version']: '00',
          },
          {
            issueID: 321n,
            orgID: 789n,
            isAdmin: null,
            bigint: null,
            timestamp: 1547253035381.101,
            bytes: null,
            intArray: null,
            json: '[1,2,3]',
            jsonb: '[{"boo":123}]',
            date: null,
            time: null,
            serial: 2n,
            ['_0_version']: '00',
          },
          {
            issueID: 456n,
            orgID: 789n,
            isAdmin: 0n,
            bigint: null,
            timestamp: null,
            bytes: Buffer.from('hello'),
            intArray: '[1,2]',
            json: null,
            jsonb: null,
            date: BigInt(Date.UTC(2003, 3, 23)),
            time: '09:10:11.123457', // PG rounds to microseconds
            serial: 3n,
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
      resultingPublications: [`_zero_metadata_${SHARD_ID}`, 'zero_public'],
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
        [`zero_${SHARD_ID}.clients`]: ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: ZERO_SCHEMA_VERSIONS_SPEC,
        ['public.users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
            },
            // Note: password is not published
            handle: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              notNull: false,
              dflt: null,
            },
          },
          oid: expect.any(Number),
          name: 'users',
          primaryKey: ['userID'],
          schema: 'public',
          publications: {['zero_custom']: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        [`zero_${SHARD_ID}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
        ['users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: false,
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
              notNull: false,
              dflt: null,
            },
          },
          name: 'users',
          primaryKey: ['userID'],
        },
      },
      upstream: {
        users: [
          {userID: 123, password: 'not-replicated', handle: '@zoot'},
          {userID: 456, password: 'super-secret', handle: '@bonk'},
        ],
      },
      replicatedData: {
        [`zero_${SHARD_ID}.clients`]: [],
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
      resultingPublications: [`_zero_metadata_${SHARD_ID}`, 'zero_custom'],
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
        [`zero_${SHARD_ID}.clients`]: ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: ZERO_SCHEMA_VERSIONS_SPEC,
        ['public.users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
            },
            // Note: password is not published
            handle: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              notNull: false,
              dflt: null,
            },
          },
          oid: expect.any(Number),
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
        [`zero_${SHARD_ID}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
        ['users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: false,
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
              notNull: false,
              dflt: null,
            },
          },
          name: 'users',
          primaryKey: ['userID'],
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
        [`zero_${SHARD_ID}.clients`]: [],
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
        `_zero_metadata_${SHARD_ID}`,
        'zero_custom',
        'zero_custom2',
      ],
    },
    {
      name: 'replicates indexes',
      setupUpstreamQuery: `
        CREATE TABLE issues(
          "issueID" INTEGER,
          "orgID" INTEGER,
          "other" TEXT,
          "isAdmin" BOOLEAN,
          PRIMARY KEY ("orgID", "issueID")
        );
        CREATE INDEX ON issues ("orgID" DESC, "other");
      `,
      published: {
        [`zero_${SHARD_ID}.clients`]: ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: ZERO_SCHEMA_VERSIONS_SPEC,
        ['public.issues']: {
          columns: {
            issueID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
            },
            other: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              notNull: false,
              dflt: null,
            },
            isAdmin: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'bool',
              typeOID: 16,
              notNull: false,
              dflt: null,
            },
          },
          oid: expect.any(Number),
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
          schema: 'public',
          publications: {['zero_public']: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        [`zero_${SHARD_ID}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        ['zero.schemaVersions']: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
        ['issues']: {
          columns: {
            issueID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: false,
              dflt: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: false,
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
              notNull: false,
              dflt: null,
            },
          },
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
        },
      },
      upstream: {},
      replicatedData: {
        [`zero_${SHARD_ID}.clients`]: [],
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
          columns: {
            orgID: 'DESC',
            other: 'ASC',
          },
          name: 'issues_orgID_other_idx',
          tableName: 'issues',
          unique: false,
        },
      ],
      resultingPublications: [`_zero_metadata_${SHARD_ID}`, 'zero_public'],
    },
  ];

  let upstream: PostgresDB;
  let replica: Database;

  beforeEach(async () => {
    upstream = await testDBs.create('initial_sync_upstream');
    replica = new Database(createSilentLogContext(), ':memory:');
  });

  afterEach(async () => {
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

      const result = await upstream.unsafe(
        `SELECT * FROM zero_${SHARD_ID}."shardConfig"`,
      );
      const tableSpecs = Object.entries(c.published)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([_, spec]) => spec);
      expect(result[0]).toMatchObject({
        publications: c.resultingPublications,
        ddlDetection: true,
        // Importantly, the initialSchema column is populated during initial sync.
        initialSchema: {
          tables: tableSpecs,
          indexes: (c.replicatedIndices ?? []).map(spec => ({
            schema: 'public',
            ...spec,
          })),
        },
      });

      const {publications, tables} = await getPublicationInfo(
        upstream,
        c.resultingPublications,
      );
      expect(
        Object.fromEntries(
          tables.map(table => [`${table.schema}.${table.name}`, table]),
        ),
      ).toMatchObject(c.published);
      expect(new Set(publications.map(p => p.pubname))).toEqual(
        new Set(c.resultingPublications),
      );

      const synced = listTables(replica);
      expect(
        Object.fromEntries(synced.map(table => [table.name, table])),
      ).toMatchObject(c.replicatedSchema);
      const {pubs} = replica
        .prepare(`SELECT publications as pubs FROM "_zero.replicationConfig"`)
        .get<{pubs: string}>();
      expect(new Set(JSON.parse(pubs))).toEqual(
        new Set(c.resultingPublications),
      );

      const syncedIndices = listIndexes(replica);
      expect(syncedIndices).toEqual(c.replicatedIndices ?? []);

      expectTables(replica, c.replicatedData, 'bigint');

      const replicaState = replica
        .prepare('SELECT * FROM "_zero.replicationState"')
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
      expectTables(replica, {['_zero.changeLog']: []});

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

  test('resume initial sync with invalid table', async () => {
    const lc = createSilentLogContext();
    const shardConfig = {id: SHARD_ID, publications: []};

    await initShardSchema(lc, upstream, shardConfig);

    // Shard should be setup to publish all "public" tables.
    // Now add an invalid table that becomes part of that publication.

    await upstream`CREATE TABLE no_primary_key(id int4)`;

    let result;
    try {
      await initialSync(lc, shardConfig, replica, getConnectionURI(upstream));
    } catch (e) {
      result = e;
    }
    expect(result).toBeInstanceOf(UnsupportedTableSchemaError);
  });

  test.each([
    'UPPERCASE',
    'dashes-not-allowed',
    'spaces not allowed',
    'punctuation!',
  ])('invalid shard ID: %s', async id => {
    const lc = createSilentLogContext();
    let result;
    try {
      await initialSync(
        lc,
        {id, publications: []},
        replica,
        getConnectionURI(upstream),
      );
    } catch (e) {
      result = e;
    }
    expect(result).toBeInstanceOf(Error);
    expect(String(result)).toEqual(
      'Error: A shard ID may only consist of lower-case letters, numbers, and the underscore character',
    );
  });
});
