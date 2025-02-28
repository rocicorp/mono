import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {listIndexes, listTables} from '../../../db/lite-tables.ts';
import {mapPostgresToLiteIndex} from '../../../db/pg-to-lite.ts';
import type {
  IndexSpec,
  LiteTableSpec,
  PublishedTableSpec,
} from '../../../db/specs.ts';
import {getConnectionURI, initDB, testDBs} from '../../../test/db.ts';
import {
  expectMatchingObjectsInTables,
  expectTables,
  initDB as initLiteDB,
} from '../../../test/lite.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {initialSync, replicationSlot} from './initial-sync.ts';
import {fromLexiVersion} from './lsn.ts';
import {initShardSchema} from './schema/init.ts';
import {getPublicationInfo} from './schema/published.ts';
import {UnsupportedTableSchemaError} from './schema/validation.ts';

const APP_ID = 'zrohs';
const SHARD_NUM = 18;

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
  publications: {[`_${APP_ID}_metadata_${SHARD_NUM}`]: {rowFilter: null}},
  schema: APP_ID,
} as const;

const ZERO_PERMISSIONS_SPEC: PublishedTableSpec = {
  columns: {
    permissions: {
      characterMaximumLength: null,
      dataType: 'jsonb',
      typeOID: 3802,
      dflt: null,
      notNull: false,
      pos: 1,
    },
    hash: {
      characterMaximumLength: null,
      dataType: 'text',
      typeOID: 25,
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
  name: 'permissions',
  primaryKey: ['lock'],
  publications: {[`_${APP_ID}_metadata_${SHARD_NUM}`]: {rowFilter: null}},
  schema: APP_ID,
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
  schema: `${APP_ID}_${SHARD_NUM}`,
  publications: {[`_${APP_ID}_metadata_${SHARD_NUM}`]: {rowFilter: null}},
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
      dataType: 'bool|NOT_NULL',
      dflt: null,
      notNull: false,
      pos: 3,
    },
  },
  name: `${APP_ID}.schemaVersions`,
} as const;

const REPLICATED_ZERO_PERMISSIONS_SPEC: LiteTableSpec = {
  columns: {
    permissions: {
      characterMaximumLength: null,
      dataType: 'jsonb',
      dflt: null,
      notNull: false,
      pos: 1,
    },
    hash: {
      characterMaximumLength: null,
      dataType: 'TEXT',
      dflt: null,
      notNull: false,
      pos: 2,
    },
    lock: {
      characterMaximumLength: null,
      dataType: 'bool|NOT_NULL',
      dflt: null,
      notNull: false,
      pos: 3,
    },
  },
  name: `${APP_ID}.permissions`,
} as const;

const REPLICATED_ZERO_CLIENTS_SPEC: LiteTableSpec = {
  columns: {
    clientGroupID: {
      pos: 1,
      characterMaximumLength: null,
      dataType: 'text|NOT_NULL',
      notNull: false,
      dflt: null,
    },
    clientID: {
      pos: 2,
      characterMaximumLength: null,
      dataType: 'text|NOT_NULL',
      notNull: false,
      dflt: null,
    },
    lastMutationID: {
      pos: 3,
      characterMaximumLength: null,
      dataType: 'int8|NOT_NULL',
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
  name: `${APP_ID}_${SHARD_NUM}.clients`,
} as const;

const WATERMARK_REGEX = /[0-9a-z]{4,}/;

describe('change-source/pg/initial-sync', {timeout: 10000}, () => {
  type Case = {
    name: string;
    setupUpstreamQuery?: string;
    requestedPublications?: string[];
    setupReplicaQuery?: string;
    published: Record<string, PublishedTableSpec>;
    upstream?: Record<string, object[]>;
    replicatedSchema: Record<string, LiteTableSpec>;
    replicatedIndexes: IndexSpec[];
    replicatedData: Record<string, object[]>;
    resultingPublications: string[];
  };

  const cases: Case[] = [
    {
      name: 'empty DB',
      published: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: ZERO_CLIENTS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        [`${APP_ID}.schemaVersions`]: ZERO_SCHEMA_VERSIONS_SPEC,
      },
      replicatedSchema: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        [`${APP_ID}.permissions`]: REPLICATED_ZERO_PERMISSIONS_SPEC,
        [`${APP_ID}.schemaVersions`]: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
      },
      replicatedIndexes: [
        {
          columns: {lock: 'ASC'},
          name: 'permissions_pkey',
          schema: APP_ID,
          tableName: 'permissions',
          unique: true,
        },
        {
          columns: {lock: 'ASC'},
          name: 'schemaVersions_pkey',
          schema: APP_ID,
          tableName: 'schemaVersions',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
      ],
      replicatedData: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        [`${APP_ID}.schemaVersions`]: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: WATERMARK_REGEX,
          },
        ],
      },
      resultingPublications: [
        `_${APP_ID}_metadata_${SHARD_NUM}`,
        `_${APP_ID}_public_${SHARD_NUM}`,
      ],
    },
    {
      name: 'replication slot already exists',
      setupUpstreamQuery: `
        SELECT * FROM pg_create_logical_replication_slot('${replicationSlot({
          appID: APP_ID,
          shardNum: SHARD_NUM,
        })}', 'pgoutput');
      `,
      published: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: ZERO_CLIENTS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        [`${APP_ID}.schemaVersions`]: ZERO_SCHEMA_VERSIONS_SPEC,
      },
      replicatedSchema: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        [`${APP_ID}.permissions`]: REPLICATED_ZERO_PERMISSIONS_SPEC,
        [`${APP_ID}.schemaVersions`]: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
      },
      replicatedIndexes: [
        {
          columns: {lock: 'ASC'},
          name: 'permissions_pkey',
          schema: APP_ID,
          tableName: 'permissions',
          unique: true,
        },
        {
          columns: {lock: 'ASC'},
          name: 'schemaVersions_pkey',
          schema: APP_ID,
          tableName: 'schemaVersions',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
      ],
      replicatedData: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        [`${APP_ID}.schemaVersions`]: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: WATERMARK_REGEX,
          },
        ],
      },
      resultingPublications: [
        `_${APP_ID}_metadata_${SHARD_NUM}`,
        `_${APP_ID}_public_${SHARD_NUM}`,
      ],
    },
    {
      name: 'existing table, default publication',
      setupUpstreamQuery: `
        CREATE TYPE ENUMZ AS ENUM ('1', '2', '3');
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
          "enumz" ENUMZ,
          "gen" INTEGER GENERATED ALWAYS AS ("orgID" + "issueID") STORED,
          "shortID" INTEGER GENERATED ALWAYS AS IDENTITY,
          "shortID2" INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 1001),
          "num" NUMERIC,
          PRIMARY KEY ("orgID", "issueID")
        );
      `,
      published: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: ZERO_CLIENTS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        [`${APP_ID}.schemaVersions`]: ZERO_SCHEMA_VERSIONS_SPEC,
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
            enumz: {
              pos: 13,
              characterMaximumLength: null,
              dataType: 'enumz',
              typeOID: expect.any(Number),
              dflt: null,
              notNull: false,
            },
            // Confirming: https://www.postgresql.org/docs/current/ddl-generated-columns.html
            // "Generated columns are skipped for logical replication"
            // gen: {
            //   pos: 14,
            //   characterMaximumLength: null,
            //   dataType: 'int4',
            //   typeOID: 23,
            //   dflt: '("orgID" + "issueID")',
            //   notNull: false,
            // },
            // Identity columns, however, are replicated.
            // https://www.postgresql.org/docs/current/ddl-identity-columns.html
            shortID: {
              pos: 15,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              dflt: null,
              notNull: true,
            },
            shortID2: {
              pos: 16,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              dflt: null,
              notNull: true,
            },
            num: {
              pos: 17,
              characterMaximumLength: null,
              dataType: 'numeric',
              typeOID: 1700,
              dflt: null,
              notNull: false,
            },
          },
          oid: expect.any(Number),
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
          schema: 'public',
          publications: {[`_${APP_ID}_public_${SHARD_NUM}`]: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        ['issues']: {
          columns: {
            issueID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
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
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
            },
            enumz: {
              pos: 13,
              characterMaximumLength: null,
              dataType: 'enumz|TEXT_ENUM',
              dflt: null,
              notNull: false,
            },
            shortID: {
              pos: 14,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
            },
            shortID2: {
              pos: 15,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
            },
            num: {
              pos: 16,
              characterMaximumLength: null,
              dataType: 'numeric',
              notNull: false,
              dflt: null,
            },
            ['_0_version']: {
              pos: 17,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              dflt: null,
            },
          },
          name: 'issues',
        },
      },
      replicatedIndexes: [
        {
          columns: {
            issueID: 'ASC',
            orgID: 'ASC',
          },
          name: 'issues_pkey',
          schema: 'public',
          tableName: 'issues',
          unique: true,
        },
        {
          columns: {lock: 'ASC'},
          name: 'permissions_pkey',
          schema: APP_ID,
          tableName: 'permissions',
          unique: true,
        },
        {
          columns: {lock: 'ASC'},
          name: 'schemaVersions_pkey',
          schema: APP_ID,
          tableName: 'schemaVersions',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
      ],
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
            num: '12345678901234',
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
            num: null,
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
            num: null,
          },
        ],
      },
      replicatedData: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
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
            shortID: 1n,
            shortID2: 1001n,
            num: 12345678901234n,
            ['_0_version']: WATERMARK_REGEX,
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
            shortID: 2n,
            shortID2: 1002n,
            num: null,
            ['_0_version']: WATERMARK_REGEX,
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
            shortID: 3n,
            shortID2: 1003n,
            num: null,
            ['_0_version']: WATERMARK_REGEX,
          },
        ],
        [`${APP_ID}.schemaVersions`]: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: WATERMARK_REGEX,
          },
        ],
      },
      resultingPublications: [
        `_${APP_ID}_metadata_${SHARD_NUM}`,
        `_${APP_ID}_public_${SHARD_NUM}`,
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
        [`${APP_ID}_${SHARD_NUM}.clients`]: ZERO_CLIENTS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        [`${APP_ID}.schemaVersions`]: ZERO_SCHEMA_VERSIONS_SPEC,
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
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        [`${APP_ID}.permissions`]: REPLICATED_ZERO_PERMISSIONS_SPEC,
        [`${APP_ID}.schemaVersions`]: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
        ['users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
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
        },
      },
      replicatedIndexes: [
        {
          columns: {userID: 'ASC'},
          name: 'users_pkey',
          schema: 'public',
          tableName: 'users',
          unique: true,
        },
        {
          columns: {lock: 'ASC'},
          name: 'permissions_pkey',
          schema: APP_ID,
          tableName: 'permissions',
          unique: true,
        },
        {
          columns: {lock: 'ASC'},
          name: 'schemaVersions_pkey',
          schema: APP_ID,
          tableName: 'schemaVersions',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
      ],
      upstream: {
        users: [
          {userID: 123, password: 'not-replicated', handle: '@zoot'},
          {userID: 456, password: 'super-secret', handle: '@bonk'},
        ],
      },
      replicatedData: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        users: [
          {
            userID: 123n,
            handle: '@zoot',
            ['_0_version']: WATERMARK_REGEX,
          },
          {
            userID: 456n,
            handle: '@bonk',
            ['_0_version']: WATERMARK_REGEX,
          },
        ],
        [`${APP_ID}.schemaVersions`]: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: WATERMARK_REGEX,
          },
        ],
      },
      resultingPublications: [
        `_${APP_ID}_metadata_${SHARD_NUM}`,
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
        [`${APP_ID}_${SHARD_NUM}.clients`]: ZERO_CLIENTS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        [`${APP_ID}.schemaVersions`]: ZERO_SCHEMA_VERSIONS_SPEC,
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
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        [`${APP_ID}.permissions`]: REPLICATED_ZERO_PERMISSIONS_SPEC,
        [`${APP_ID}.schemaVersions`]: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
        ['users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
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
        },
      },
      replicatedIndexes: [
        {
          columns: {userID: 'ASC'},
          name: 'users_pkey',
          schema: 'public',
          tableName: 'users',
          unique: true,
        },
        {
          columns: {lock: 'ASC'},
          name: 'permissions_pkey',
          schema: APP_ID,
          tableName: 'permissions',
          unique: true,
        },
        {
          columns: {lock: 'ASC'},
          name: 'schemaVersions_pkey',
          schema: APP_ID,
          tableName: 'schemaVersions',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
      ],
      upstream: {
        users: [
          {userID: 123, password: 'not-replicated', handle: '@zoot'},
          {userID: 456, password: 'super-secret', handle: '@bonk'},
          {userID: 1001, password: 'hide-me', handle: '@boom'},
        ],
      },
      replicatedData: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        users: [
          {
            userID: 456n,
            handle: '@bonk',
            ['_0_version']: WATERMARK_REGEX,
          },
          {
            userID: 1001n,
            handle: '@boom',
            ['_0_version']: WATERMARK_REGEX,
          },
        ],
        [`${APP_ID}.schemaVersions`]: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: WATERMARK_REGEX,
          },
        ],
      },
      resultingPublications: [
        `_${APP_ID}_metadata_${SHARD_NUM}`,
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
        [`${APP_ID}_${SHARD_NUM}.clients`]: ZERO_CLIENTS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        [`${APP_ID}.schemaVersions`]: ZERO_SCHEMA_VERSIONS_SPEC,
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
          publications: {[`_${APP_ID}_public_${SHARD_NUM}`]: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        [`${APP_ID}.schemaVersions`]: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
        ['issues']: {
          columns: {
            issueID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
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
        },
      },
      upstream: {},
      replicatedData: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        issues: [],
        [`${APP_ID}.schemaVersions`]: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: WATERMARK_REGEX,
          },
        ],
      },
      replicatedIndexes: [
        {
          columns: {
            orgID: 'DESC',
            other: 'ASC',
          },
          name: 'issues_orgID_other_idx',
          schema: 'public',
          tableName: 'issues',
          unique: false,
        },
        {
          columns: {
            issueID: 'ASC',
            orgID: 'ASC',
          },
          name: 'issues_pkey',
          schema: 'public',
          tableName: 'issues',
          unique: true,
        },
        {
          columns: {lock: 'ASC'},
          name: 'permissions_pkey',
          schema: APP_ID,
          tableName: 'permissions',
          unique: true,
        },
        {
          columns: {lock: 'ASC'},
          name: 'schemaVersions_pkey',
          schema: APP_ID,
          tableName: 'schemaVersions',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
      ],
      resultingPublications: [
        `_${APP_ID}_metadata_${SHARD_NUM}`,
        `_${APP_ID}_public_${SHARD_NUM}`,
      ],
    },
    {
      name: 'partitioned table',
      setupUpstreamQuery: `
        CREATE TABLE giant(id INTEGER PRIMARY KEY) PARTITION BY HASH (id);
        CREATE TABLE giant_default PARTITION OF giant
          FOR VALUES WITH (MODULUS 1, REMAINDER 0);
      `,
      published: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: ZERO_CLIENTS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        [`${APP_ID}.schemaVersions`]: ZERO_SCHEMA_VERSIONS_SPEC,
        ['public.giant']: {
          columns: {
            id: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
            },
          },
          oid: expect.any(Number),
          name: 'giant',
          primaryKey: ['id'],
          schema: 'public',
          publications: {[`_${APP_ID}_public_${SHARD_NUM}`]: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        [`${APP_ID}.permissions`]: REPLICATED_ZERO_PERMISSIONS_SPEC,
        [`${APP_ID}.schemaVersions`]: REPLICATED_ZERO_SCHEMA_VERSIONS_SPEC,
        ['giant']: {
          columns: {
            id: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
            },
            ['_0_version']: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              dflt: null,
            },
          },
          name: 'giant',
        },
      },
      replicatedIndexes: [
        {
          columns: {id: 'ASC'},
          name: 'giant_pkey',
          schema: 'public',
          tableName: 'giant',
          unique: true,
        },
        {
          columns: {lock: 'ASC'},
          name: 'permissions_pkey',
          schema: APP_ID,
          tableName: 'permissions',
          unique: true,
        },
        {
          columns: {lock: 'ASC'},
          name: 'schemaVersions_pkey',
          schema: APP_ID,
          tableName: 'schemaVersions',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
      ],
      upstream: {
        ['giant_default']: [{id: 123}],
      },
      replicatedData: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        [`${APP_ID}.schemaVersions`]: [
          {
            lock: 1n,
            minSupportedVersion: 1n,
            maxSupportedVersion: 1n,
            ['_0_version']: WATERMARK_REGEX,
          },
        ],
        giant: [{id: 123n}],
      },
      resultingPublications: [
        `_${APP_ID}_metadata_${SHARD_NUM}`,
        `_${APP_ID}_public_${SHARD_NUM}`,
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
    await testDBs.drop(upstream);
  });

  for (const c of cases) {
    test(`startInitialDataSynchronization: ${c.name}`, async () => {
      await initDB(upstream, c.setupUpstreamQuery, c.upstream);
      initLiteDB(replica, c.setupReplicaQuery);

      const lc = createSilentLogContext();
      await initialSync(
        lc,
        {
          appID: APP_ID,
          shardNum: SHARD_NUM,
          publications: c.requestedPublications ?? [],
        },
        replica,
        getConnectionURI(upstream),
        {tableCopyWorkers: 5, rowBatchSize: 10000},
      );

      const result = await upstream.unsafe(
        `SELECT * FROM ${APP_ID}_${SHARD_NUM}."shardConfig"`,
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
          indexes: c.replicatedIndexes,
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
      expect(syncedIndices).toEqual(
        c.replicatedIndexes.map(idx => mapPostgresToLiteIndex(idx)),
      );

      expectMatchingObjectsInTables(replica, c.replicatedData, 'bigint');

      const replicaState = replica
        .prepare('SELECT * FROM "_zero.replicationState"')
        .get<{stateVersion: string}>();
      expect(replicaState).toMatchObject({stateVersion: WATERMARK_REGEX});
      expectTables(replica, {['_zero.changeLog']: []});

      // Check replica state against the upstream slot.
      const slots = await upstream`
        SELECT slot_name as "slotName", confirmed_flush_lsn as lsn 
          FROM pg_replication_slots WHERE slot_name = ${replicationSlot({
            appID: APP_ID,
            shardNum: SHARD_NUM,
          })}`;
      expect(slots[0]).toEqual({
        slotName: replicationSlot({appID: APP_ID, shardNum: SHARD_NUM}),
        lsn: fromLexiVersion(replicaState.stateVersion),
      });
    });
  }

  test('resume initial sync with invalid table', async () => {
    const lc = createSilentLogContext();
    const shardConfig = {appID: APP_ID, shardNum: SHARD_NUM, publications: []};

    await initShardSchema(lc, upstream, shardConfig);

    // Shard should be setup to publish all "public" tables.
    // Now add an invalid table that becomes part of that publication.

    await upstream`CREATE TABLE "has/inval!d/ch@aracter$"(id int4)`;

    let result;
    try {
      await initialSync(lc, shardConfig, replica, getConnectionURI(upstream), {
        tableCopyWorkers: 5,
        rowBatchSize: 10000,
      });
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
  ])('invalid app ID: %s', async appID => {
    const lc = createSilentLogContext();
    let result;
    try {
      await initialSync(
        lc,
        {appID, shardNum: 0, publications: []},
        replica,
        getConnectionURI(upstream),
        {tableCopyWorkers: 5, rowBatchSize: 10000},
      );
    } catch (e) {
      result = e;
    }
    expect(result).toBeInstanceOf(Error);
    expect(String(result)).toEqual(
      'Error: The App ID may only consist of lower-case letters, numbers, and the underscore character',
    );
  });
});
