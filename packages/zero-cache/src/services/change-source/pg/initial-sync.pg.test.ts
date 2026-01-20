import {nanoid} from 'nanoid/non-secure';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import type {ZeroEvent} from '../../../../../zero-events/src/index.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {listIndexes, listTables} from '../../../db/lite-tables.ts';
import {mapPostgresToLiteIndex} from '../../../db/pg-to-lite.ts';
import type {
  IndexSpec,
  LiteTableSpec,
  PublishedTableSpec,
} from '../../../db/specs.ts';
import {initEventSinkForTesting} from '../../../observability/events.ts';
import {getConnectionURI, initDB, type PgTest, test} from '../../../test/db.ts';
import {
  expectMatchingObjectsInTables,
  expectTables,
  initDB as initLiteDB,
} from '../../../test/lite.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../../replicator/schema/replication-state.ts';
import {initialSync, INSERT_BATCH_SIZE} from './initial-sync.ts';
import {fromLexiVersion} from './lsn.ts';
import {ensureShardSchema} from './schema/init.ts';
import {getPublicationInfo} from './schema/published.ts';
import {UnsupportedTableSchemaError} from './schema/validation.ts';

const APP_ID = '1';
const SHARD_NUM = 18;

const ZERO_PERMISSIONS_SPEC: PublishedTableSpec = {
  columns: {
    permissions: {
      characterMaximumLength: null,
      dataType: 'jsonb',
      typeOID: 3802,
      dflt: null,
      elemPgTypeClass: null,
      notNull: false,
      pos: 1,
    },
    hash: {
      characterMaximumLength: null,
      dataType: 'text',
      typeOID: 25,
      dflt: null,
      elemPgTypeClass: null,
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
  schemaOID: expect.any(Number),
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
      elemPgTypeClass: null,
    },
    clientID: {
      pos: 2,
      characterMaximumLength: null,
      dataType: 'text',
      typeOID: 25,
      notNull: true,
      dflt: null,
      elemPgTypeClass: null,
    },
    lastMutationID: {
      pos: 3,
      characterMaximumLength: null,
      dataType: 'int8',
      typeOID: 20,
      notNull: true,
      dflt: null,
      elemPgTypeClass: null,
    },
    userID: {
      pos: 4,
      characterMaximumLength: null,
      dataType: 'text',
      typeOID: 25,
      notNull: false,
      dflt: null,
      elemPgTypeClass: null,
    },
  },
  oid: expect.any(Number),
  name: 'clients',
  primaryKey: ['clientGroupID', 'clientID'],
  schema: `${APP_ID}_${SHARD_NUM}`,
  schemaOID: expect.any(Number),
  publications: {[`_${APP_ID}_metadata_${SHARD_NUM}`]: {rowFilter: null}},
} as const;

const ZERO_MUTATIONS_SPEC: PublishedTableSpec = {
  columns: {
    clientGroupID: {
      pos: 1,
      characterMaximumLength: null,
      dataType: 'text',
      typeOID: 25,
      notNull: true,
      dflt: null,
      elemPgTypeClass: null,
    },
    clientID: {
      pos: 2,
      characterMaximumLength: null,
      dataType: 'text',
      typeOID: 25,
      notNull: true,
      dflt: null,
      elemPgTypeClass: null,
    },
    mutationID: {
      pos: 3,
      characterMaximumLength: null,
      dataType: 'int8',
      typeOID: 20,
      notNull: true,
      dflt: null,
      elemPgTypeClass: null,
    },
    result: {
      pos: 4,
      characterMaximumLength: null,
      dataType: 'json',
      typeOID: 114,
      notNull: true,
      dflt: null,
      elemPgTypeClass: null,
    },
  },
  oid: expect.any(Number),
  name: 'mutations',
  primaryKey: ['clientGroupID', 'clientID', 'mutationID'],
  schema: `${APP_ID}_${SHARD_NUM}`,
  schemaOID: expect.any(Number),
  publications: {[`_${APP_ID}_metadata_${SHARD_NUM}`]: {rowFilter: null}},
} as const;

const REPLICATED_ZERO_PERMISSIONS_SPEC: LiteTableSpec = {
  columns: {
    permissions: {
      characterMaximumLength: null,
      dataType: 'jsonb',
      dflt: null,
      elemPgTypeClass: null,
      notNull: false,
      pos: 1,
    },
    hash: {
      characterMaximumLength: null,
      dataType: 'TEXT',
      dflt: null,
      elemPgTypeClass: null,
      notNull: false,
      pos: 2,
    },
    lock: {
      characterMaximumLength: null,
      dataType: 'bool|NOT_NULL',
      dflt: null,
      elemPgTypeClass: null,
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
      elemPgTypeClass: null,
    },
    clientID: {
      pos: 2,
      characterMaximumLength: null,
      dataType: 'text|NOT_NULL',
      notNull: false,
      dflt: null,
      elemPgTypeClass: null,
    },
    lastMutationID: {
      pos: 3,
      characterMaximumLength: null,
      dataType: 'int8|NOT_NULL',
      notNull: false,
      dflt: null,
      elemPgTypeClass: null,
    },
    userID: {
      pos: 4,
      characterMaximumLength: null,
      dataType: 'TEXT',
      notNull: false,
      dflt: null,
      elemPgTypeClass: null,
    },
  },
  name: `${APP_ID}_${SHARD_NUM}.clients`,
} as const;

const REPLICATED_ZERO_MUTATIONS_SPEC: LiteTableSpec = {
  columns: {
    clientGroupID: {
      pos: 1,
      characterMaximumLength: null,
      dataType: 'text|NOT_NULL',
      notNull: false,
      dflt: null,
      elemPgTypeClass: null,
    },
    clientID: {
      pos: 2,
      characterMaximumLength: null,
      dataType: 'text|NOT_NULL',
      notNull: false,
      dflt: null,
      elemPgTypeClass: null,
    },
    mutationID: {
      pos: 3,
      characterMaximumLength: null,
      dataType: 'int8|NOT_NULL',
      notNull: false,
      dflt: null,
      elemPgTypeClass: null,
    },
    result: {
      pos: 4,
      characterMaximumLength: null,
      dataType: 'json|NOT_NULL',
      notNull: false,
      dflt: null,
      elemPgTypeClass: null,
    },
  },
  name: `${APP_ID}_${SHARD_NUM}.mutations`,
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
    minPgVersion?: number;
  };

  const cases: Case[] = [
    {
      name: 'empty DB',
      published: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: ZERO_CLIENTS_SPEC,
        [`${APP_ID}_${SHARD_NUM}.mutations`]: ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
      },
      replicatedSchema: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        [`${APP_ID}_${SHARD_NUM}.mutations`]: REPLICATED_ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: REPLICATED_ZERO_PERMISSIONS_SPEC,
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
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
            mutationID: 'ASC',
          },
          name: 'mutations_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'mutations',
          unique: true,
        },
      ],
      replicatedData: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        [`${APP_ID}_${SHARD_NUM}.mutations`]: [],
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
          "column.with.dots" TEXT,
          PRIMARY KEY ("orgID", "issueID")
        );
      `,
      published: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: ZERO_CLIENTS_SPEC,
        [`${APP_ID}_${SHARD_NUM}.mutations`]: ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        ['public.issues']: {
          columns: {
            issueID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
            isAdmin: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'bool',
              typeOID: 16,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            bigint: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'int8',
              typeOID: 20,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            timestamp: {
              pos: 5,
              characterMaximumLength: null,
              dataType: 'timestamptz',
              typeOID: 1184,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            bytes: {
              pos: 6,
              characterMaximumLength: null,
              dataType: 'bytea',
              typeOID: 17,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            intArray: {
              pos: 7,
              characterMaximumLength: null,
              dataType: 'int4[]',
              typeOID: 1007,
              notNull: false,
              dflt: null,
              elemPgTypeClass: 'b',
            },
            json: {
              pos: 8,
              characterMaximumLength: null,
              dataType: 'json',
              typeOID: 114,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            jsonb: {
              pos: 9,
              characterMaximumLength: null,
              dataType: 'jsonb',
              typeOID: 3802,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            date: {
              pos: 10,
              characterMaximumLength: null,
              dataType: 'date',
              typeOID: 1082,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            time: {
              pos: 11,
              characterMaximumLength: null,
              dataType: 'time',
              typeOID: 1083,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
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
              elemPgTypeClass: null,
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
              elemPgTypeClass: null,
              notNull: true,
            },
            shortID2: {
              pos: 16,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              dflt: null,
              elemPgTypeClass: null,
              notNull: true,
            },
            num: {
              pos: 17,
              characterMaximumLength: null,
              dataType: 'numeric',
              typeOID: 1700,
              dflt: null,
              elemPgTypeClass: null,
              notNull: false,
            },
            ['column.with.dots']: {
              pos: 18,
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              dflt: null,
              elemPgTypeClass: null,
              notNull: false,
            },
          },
          oid: expect.any(Number),
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
          schema: 'public',
          schemaOID: expect.any(Number),
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
              elemPgTypeClass: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            isAdmin: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'bool',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            bigint: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'int8',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            timestamp: {
              pos: 5,
              characterMaximumLength: null,
              dataType: 'timestamptz',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            bytes: {
              pos: 6,
              characterMaximumLength: null,
              dataType: 'bytea',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            intArray: {
              pos: 7,
              characterMaximumLength: null,
              dataType: 'int4[]|TEXT_ARRAY',
              notNull: false,
              dflt: null,
              elemPgTypeClass: 'b',
            },
            json: {
              pos: 8,
              characterMaximumLength: null,
              dataType: 'json',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            jsonb: {
              pos: 9,
              characterMaximumLength: null,
              dataType: 'jsonb',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            date: {
              pos: 10,
              characterMaximumLength: null,
              dataType: 'date',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            time: {
              pos: 11,
              characterMaximumLength: null,
              dataType: 'time',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            serial: {
              pos: 12,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            enumz: {
              pos: 13,
              characterMaximumLength: null,
              dataType: 'enumz|TEXT_ENUM',
              dflt: null,
              elemPgTypeClass: null,
              notNull: false,
            },
            shortID: {
              pos: 14,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            shortID2: {
              pos: 15,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            num: {
              pos: 16,
              characterMaximumLength: null,
              dataType: 'numeric',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            ['column.with.dots']: {
              pos: 17,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            ['_0_version']: {
              pos: 18,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              elemPgTypeClass: null,
            },
          },
          name: 'issues',
        },
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
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
            mutationID: 'ASC',
          },
          name: 'mutations_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'mutations',
          unique: true,
        },
        {
          columns: {
            orgID: 'ASC',
            issueID: 'ASC',
          },
          name: 'issues_pkey',
          schema: 'public',
          tableName: 'issues',
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
            jsonb: '{"bar": "baz"}',
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
            jsonb: '[{"boo": 123}]',
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
            time: 33011123n, // Convert 09:10:11.123456789 to milliseconds since midnight
            serial: 3n,
            shortID: 3n,
            shortID2: 1003n,
            num: null,
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
      name: 'batch inserts',
      setupUpstreamQuery: `
        CREATE TABLE foo(
          "id" TEXT PRIMARY KEY,
          "bigint" BIGINT,
          "timestamp" TIMESTAMPTZ,
          "bytes" BYTEA,
          "jsonb" JSONB
        );
      `,
      published: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: ZERO_CLIENTS_SPEC,
        [`${APP_ID}_${SHARD_NUM}.mutations`]: ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        ['public.foo']: {
          columns: {
            id: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
            bigint: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int8',
              typeOID: 20,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            timestamp: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'timestamptz',
              typeOID: 1184,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            bytes: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'bytea',
              typeOID: 17,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            jsonb: {
              pos: 5,
              characterMaximumLength: null,
              dataType: 'jsonb',
              typeOID: 3802,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
          },
          oid: expect.any(Number),
          name: 'foo',
          primaryKey: ['id'],
          schema: 'public',
          schemaOID: expect.any(Number),
          publications: {[`_${APP_ID}_public_${SHARD_NUM}`]: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        ['foo']: {
          columns: {
            id: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            bigint: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int8',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            timestamp: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'timestamptz',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            bytes: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'bytea',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            jsonb: {
              pos: 5,
              characterMaximumLength: null,
              dataType: 'jsonb',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            ['_0_version']: {
              pos: 6,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              elemPgTypeClass: null,
            },
          },
          name: 'foo',
        },
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
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
            mutationID: 'ASC',
          },
          name: 'mutations_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'mutations',
          unique: true,
        },
        {
          columns: {id: 'ASC'},
          name: 'foo_pkey',
          schema: 'public',
          tableName: 'foo',
          unique: true,
        },
      ],
      resultingPublications: [
        `_${APP_ID}_metadata_${SHARD_NUM}`,
        `_${APP_ID}_public_${SHARD_NUM}`,
      ],
      upstream: {
        foo: Array.from(
          {length: Math.floor(INSERT_BATCH_SIZE * 2.23)},
          (_, i) => ({
            id: '' + i,
            bigint: BigInt(Number.MAX_SAFE_INTEGER) + BigInt(i),
            timestamp: new Date(Date.UTC(2025, 0, 1, 0, 0, i)).toISOString(),
            bytes: Buffer.from('hello' + i),
            jsonb: {foo: i},
          }),
        ),
      },
      replicatedData: {
        foo: Array.from(
          {length: Math.floor(INSERT_BATCH_SIZE * 2.23)},
          (_, i) => ({
            id: '' + i,
            bigint: BigInt(Number.MAX_SAFE_INTEGER) + BigInt(i),
            timestamp: BigInt(Date.UTC(2025, 0, 1, 0, 0, i)),
            bytes: Buffer.from('hello' + i),
            jsonb: `{"foo": ${i}}`,
            [ZERO_VERSION_COLUMN_NAME]: WATERMARK_REGEX,
          }),
        ),
      },
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
        [`${APP_ID}_${SHARD_NUM}.mutations`]: ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        ['public.users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
            // Note: password is not published
            handle: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
          },
          oid: expect.any(Number),
          name: 'users',
          primaryKey: ['userID'],
          schema: 'public',
          schemaOID: expect.any(Number),
          publications: {['zero_custom']: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        [`${APP_ID}_${SHARD_NUM}.mutations`]: REPLICATED_ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: REPLICATED_ZERO_PERMISSIONS_SPEC,
        ['users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            // Note: password is not published
            handle: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            ['_0_version']: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              elemPgTypeClass: null,
            },
          },
          name: 'users',
        },
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
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
            mutationID: 'ASC',
          },
          name: 'mutations_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'mutations',
          unique: true,
        },
        {
          columns: {userID: 'ASC'},
          name: 'users_pkey',
          schema: 'public',
          tableName: 'users',
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
        [`${APP_ID}_${SHARD_NUM}.mutations`]: ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        ['public.users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
            // Note: password is not published
            handle: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
          },
          oid: expect.any(Number),
          name: 'users',
          primaryKey: ['userID'],
          schema: 'public',
          schemaOID: expect.any(Number),
          publications: {
            ['zero_custom']: {rowFilter: '(("userID" % 2) = 0)'},
            ['zero_custom2']: {rowFilter: '("userID" > 1000)'},
          },
        },
      },
      replicatedSchema: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        [`${APP_ID}_${SHARD_NUM}.mutations`]: REPLICATED_ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: REPLICATED_ZERO_PERMISSIONS_SPEC,
        ['users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            // Note: password is not published
            handle: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            ['_0_version']: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              elemPgTypeClass: null,
            },
          },
          name: 'users',
        },
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
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
            mutationID: 'ASC',
          },
          name: 'mutations_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'mutations',
          unique: true,
        },
        {
          columns: {userID: 'ASC'},
          name: 'users_pkey',
          schema: 'public',
          tableName: 'users',
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
      },
      resultingPublications: [
        `_${APP_ID}_metadata_${SHARD_NUM}`,
        'zero_custom',
        'zero_custom2',
      ],
    },
    {
      name: 'publication with generated columns',
      minPgVersion: 180000,
      setupUpstreamQuery: `
        CREATE TABLE users(
          "userID" INTEGER, 
          handle TEXT, 
          gen INTEGER GENERATED ALWAYS AS ("userID" + 1) STORED,
          PRIMARY KEY ("userID"));
        CREATE INDEX on users (handle, gen);
        CREATE PUBLICATION zero_custom FOR TABLE users WITH (publish_generated_columns=stored);
      `,
      requestedPublications: ['zero_custom'],
      published: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: ZERO_CLIENTS_SPEC,
        [`${APP_ID}_${SHARD_NUM}.mutations`]: ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        ['public.users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
            handle: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            gen: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: false,
              dflt: '("userID" + 1)',
              elemPgTypeClass: null,
            },
          },
          oid: expect.any(Number),
          name: 'users',
          primaryKey: ['userID'],
          schema: 'public',
          schemaOID: expect.any(Number),
          publications: {['zero_custom']: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        [`${APP_ID}_${SHARD_NUM}.mutations`]: REPLICATED_ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: REPLICATED_ZERO_PERMISSIONS_SPEC,
        ['users']: {
          columns: {
            userID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            handle: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            gen: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'int4',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            ['_0_version']: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              elemPgTypeClass: null,
            },
          },
          name: 'users',
        },
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
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
            mutationID: 'ASC',
          },
          name: 'mutations_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'mutations',
          unique: true,
        },
        {
          columns: {
            handle: 'ASC',
            gen: 'ASC',
          },
          name: 'users_handle_gen_idx',
          schema: 'public',
          tableName: 'users',
          unique: false,
        },
        {
          columns: {userID: 'ASC'},
          name: 'users_pkey',
          schema: 'public',
          tableName: 'users',
          unique: true,
        },
      ],
      upstream: {
        users: [
          {userID: 123, handle: '@zoot'},
          {userID: 456, handle: '@bonk'},
        ],
      },
      replicatedData: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        users: [
          {
            userID: 123n,
            handle: '@zoot',
            gen: 124n,
            ['_0_version']: WATERMARK_REGEX,
          },
          {
            userID: 456n,
            handle: '@bonk',
            gen: 457n,
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
        [`${APP_ID}_${SHARD_NUM}.mutations`]: ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        ['public.issues']: {
          columns: {
            issueID: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
            other: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            isAdmin: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'bool',
              typeOID: 16,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
          },
          oid: expect.any(Number),
          name: 'issues',
          primaryKey: ['orgID', 'issueID'],
          schema: 'public',
          schemaOID: expect.any(Number),
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
              elemPgTypeClass: null,
            },
            orgID: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            other: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            isAdmin: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'bool',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            ['_0_version']: {
              pos: 5,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              elemPgTypeClass: null,
            },
          },
          name: 'issues',
        },
      },
      upstream: {},
      replicatedData: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        issues: [],
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
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
            mutationID: 'ASC',
          },
          name: 'mutations_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'mutations',
          unique: true,
        },
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
            orgID: 'ASC',
            issueID: 'ASC',
          },
          name: 'issues_pkey',
          schema: 'public',
          tableName: 'issues',
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
        [`${APP_ID}_${SHARD_NUM}.mutations`]: ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        ['public.giant']: {
          columns: {
            id: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
          },
          oid: expect.any(Number),
          name: 'giant',
          primaryKey: ['id'],
          schema: 'public',
          schemaOID: expect.any(Number),
          publications: {[`_${APP_ID}_public_${SHARD_NUM}`]: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        [`${APP_ID}_${SHARD_NUM}.mutations`]: REPLICATED_ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: REPLICATED_ZERO_PERMISSIONS_SPEC,
        ['giant']: {
          columns: {
            id: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            ['_0_version']: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              elemPgTypeClass: null,
            },
          },
          name: 'giant',
        },
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
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
            mutationID: 'ASC',
          },
          name: 'mutations_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'mutations',
          unique: true,
        },
        {
          columns: {id: 'ASC'},
          name: 'giant_pkey',
          schema: 'public',
          tableName: 'giant',
          unique: true,
        },
      ],
      upstream: {
        ['giant_default']: [{id: 123}],
      },
      replicatedData: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        giant: [{id: 123n}],
      },
      resultingPublications: [
        `_${APP_ID}_metadata_${SHARD_NUM}`,
        `_${APP_ID}_public_${SHARD_NUM}`,
      ],
    },
    {
      name: 'unique constraints',
      setupUpstreamQuery: /*sql*/ `
        CREATE TABLE "funk" (
          "id" text PRIMARY KEY NOT NULL,
          "name" varchar(255) NOT NULL,
          "order" integer DEFAULT 0 NOT NULL,
          "createdAt" timestamp DEFAULT now() NOT NULL,
          "updatedAt" timestamp DEFAULT now() NOT NULL,
          CONSTRAINT "funk_name_unique" UNIQUE("name"),
          CONSTRAINT "funk_order_unique" UNIQUE("order")
        );
      `,
      published: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: ZERO_CLIENTS_SPEC,
        [`${APP_ID}_${SHARD_NUM}.mutations`]: ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        ['public.funk']: {
          columns: {
            id: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
            name: {
              pos: 2,
              characterMaximumLength: 255,
              dataType: 'varchar',
              typeOID: 1043,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
            order: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'int4',
              typeOID: 23,
              notNull: true,
              dflt: '0',
            },
            createdAt: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'timestamp',
              typeOID: 1114,
              notNull: true,
              dflt: 'now()',
            },
            updatedAt: {
              pos: 5,
              characterMaximumLength: null,
              dataType: 'timestamp',
              typeOID: 1114,
              notNull: true,
              dflt: 'now()',
            },
          },
          oid: expect.any(Number),
          name: 'funk',
          primaryKey: ['id'],
          schema: 'public',
          schemaOID: expect.any(Number),
          publications: {[`_${APP_ID}_public_${SHARD_NUM}`]: {rowFilter: null}},
        },
      },
      replicatedSchema: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        ['funk']: {
          columns: {
            id: {
              pos: 1,
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            name: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'varchar|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            order: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            createdAt: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'timestamp|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            updatedAt: {
              pos: 5,
              characterMaximumLength: null,
              dataType: 'timestamp|NOT_NULL',
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            ['_0_version']: {
              pos: 6,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
              elemPgTypeClass: null,
            },
          },
          name: 'funk',
        },
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
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
            mutationID: 'ASC',
          },
          name: 'mutations_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'mutations',
          unique: true,
        },
        {
          columns: {name: 'ASC'},
          name: 'funk_name_unique',
          schema: 'public',
          tableName: 'funk',
          unique: true,
        },
        {
          columns: {order: 'ASC'},
          name: 'funk_order_unique',
          schema: 'public',
          tableName: 'funk',
          unique: true,
        },
        {
          columns: {id: 'ASC'},
          name: 'funk_pkey',
          schema: 'public',
          tableName: 'funk',
          unique: true,
        },
      ],
      resultingPublications: [
        `_${APP_ID}_metadata_${SHARD_NUM}`,
        `_${APP_ID}_public_${SHARD_NUM}`,
      ],
      upstream: {
        funk: [
          {
            id: '123',
            name: '456',
            order: 1,
            createdAt: '2019-01-12T00:30:35.381101032Z',
            updatedAt: '2019-01-12T00:30:35.381101032Z',
          },
        ],
      },
      replicatedData: {
        funk: [
          {
            createdAt: 1547253035381.101,
            id: '123',
            name: '456',
            order: 1n,
            updatedAt: 1547253035381.101,
            ['_0_version']: WATERMARK_REGEX,
          },
        ],
      },
    },
    {
      name: 'multiple copy commands per copy worker',
      setupUpstreamQuery: Array.from({length: 10}, (_, i) =>
        [
          // This is a carefully crafted test that hits a bug in Postgres in which
          // the database stops responding to commands after a certain type/sequence
          // of COPY streams.
          //
          // The following conditions appear to be necessary to trigger the bug:
          // - Sufficient table data streamed from the COPY (hence the 400 rows and `b` column)
          // - A randomly ordered primary key column (the `a` column)
          // - More tables than copy workers, to exercise post-COPY commands (hence the 10 tables).
          //
          // A failure manifests as a "Test timed out" error as Postgres becomes unresponsive.
          `CREATE TABLE t${i} (a TEXT PRIMARY KEY, b TEXT, val INT);`,
          ...Array.from(
            {length: 400},
            (_, r) =>
              `INSERT INTO t${i} (a, b, val) VALUES ('${nanoid()}', '0000000000000000', ${r});`,
          ),
        ].join('\n'),
      ).join('\n'),
      published: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: ZERO_CLIENTS_SPEC,
        [`${APP_ID}_${SHARD_NUM}.mutations`]: ZERO_MUTATIONS_SPEC,
        [`${APP_ID}.permissions`]: ZERO_PERMISSIONS_SPEC,
        ...Object.fromEntries(
          Array.from({length: 10}, (_, i) => [
            `public.t${i}`,
            {
              columns: {
                a: {
                  pos: 1,
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  notNull: true,
                  dflt: null,
                },
                b: {
                  pos: 2,
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  notNull: false,
                  dflt: null,
                },
                val: {
                  pos: 3,
                  characterMaximumLength: null,
                  dataType: 'int4',
                  typeOID: 23,
                  notNull: false,
                  dflt: null,
                },
              },
              oid: expect.any(Number),
              name: `t${i}`,
              primaryKey: ['a'],
              schema: 'public',
              publications: {
                [`_${APP_ID}_public_${SHARD_NUM}`]: {rowFilter: null},
              },
            },
          ]),
        ),
      },
      replicatedSchema: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: REPLICATED_ZERO_CLIENTS_SPEC,
        ...Object.fromEntries(
          Array.from({length: 10}, (_, i) => [
            `t${i}`,
            {
              columns: {
                a: {
                  pos: 1,
                  characterMaximumLength: null,
                  dataType: 'text|NOT_NULL',
                  notNull: false,
                  dflt: null,
                },
                b: {
                  pos: 2,
                  characterMaximumLength: null,
                  dataType: 'TEXT',
                  notNull: false,
                  dflt: null,
                },
                val: {
                  pos: 3,
                  characterMaximumLength: null,
                  dataType: 'int4',
                  notNull: false,
                  dflt: null,
                },
                ['_0_version']: {
                  pos: 4,
                  characterMaximumLength: null,
                  dataType: 'TEXT',
                  notNull: false,
                },
              },
              name: `t${i}`,
            },
          ]),
        ),
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
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
          },
          name: 'clients_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'clients',
          unique: true,
        },
        {
          columns: {
            clientGroupID: 'ASC',
            clientID: 'ASC',
            mutationID: 'ASC',
          },
          name: 'mutations_pkey',
          schema: `${APP_ID}_${SHARD_NUM}`,
          tableName: 'mutations',
          unique: true,
        },
        ...Array.from(
          {length: 10},
          (_, i) =>
            ({
              columns: {a: 'ASC'},
              name: `t${i}_pkey`,
              schema: 'public',
              tableName: `t${i}`,
              unique: true,
            }) as const,
        ),
      ],
      resultingPublications: [
        `_${APP_ID}_metadata_${SHARD_NUM}`,
        `_${APP_ID}_public_${SHARD_NUM}`,
      ],
      replicatedData: Object.fromEntries(
        Array.from({length: 10}, (_, i) => [
          `t${i}`,
          Array.from({length: 400}, (_, i) => ({
            b: '0000000000000000',
            val: BigInt(i),
          })),
        ]),
      ),
    },
  ];

  let upstream: PostgresDB;
  let pgVersion: number;

  beforeEach<PgTest>(async ({testDBs}) => {
    upstream = await testDBs.create('initial_sync_upstream');
    [{pgVersion}] =
      await upstream`SELECT current_setting('server_version_num')::int as "pgVersion"`;

    return () => testDBs.drop(upstream);
  });

  for (const c of cases) {
    test(c.name, async ({skip}) => {
      if (pgVersion < (c.minPgVersion ?? 0)) {
        skip();
      }
      await initDB(upstream, c.setupUpstreamQuery, c.upstream);

      const lc = createSilentLogContext();

      // Run each test twice to confirm that a takeover initial sync works.
      for (let i = 0; i < 2; i++) {
        const eventSink: ZeroEvent[] = [];
        initEventSinkForTesting(eventSink);

        const replica = new Database(lc, ':memory:');
        initLiteDB(replica, c.setupReplicaQuery);

        await initialSync(
          lc,
          {
            appID: APP_ID,
            shardNum: SHARD_NUM,
            publications: c.requestedPublications ?? [],
          },
          replica,
          getConnectionURI(upstream),
          {tableCopyWorkers: 3},
        );

        const config = await upstream.unsafe(
          `SELECT * FROM "${APP_ID}_${SHARD_NUM}"."shardConfig"`,
        );
        expect(config[0]).toMatchObject({
          publications: c.resultingPublications,
          ddlDetection: true,
        });
        const replicas = await upstream.unsafe(
          `SELECT * FROM "${APP_ID}_${SHARD_NUM}"."replicas"`,
        );
        expect(replicas).toHaveLength(i + 1);
        const tableSpecs = Object.entries(c.published)
          .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
          .map(([_, spec]) => spec);
        for (const r of replicas) {
          expect(r).toMatchObject({
            slot: expect.stringMatching(`${APP_ID}_${SHARD_NUM}_\\d+`),
            // Importantly, the initialSchema column is populated during initial sync.
            initialSchema: {
              tables: tableSpecs,
              indexes: c.replicatedIndexes,
            },
            version: WATERMARK_REGEX,
          });
        }

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

        const syncedIndexes = listIndexes(replica);
        // Test stringified indexes to verify field ordering.
        expect(JSON.stringify(syncedIndexes, null, 2)).toEqual(
          JSON.stringify(
            c.replicatedIndexes.map(idx => mapPostgresToLiteIndex(idx)),
            null,
            2,
          ),
        );

        expectMatchingObjectsInTables(replica, c.replicatedData, 'bigint');

        const replicaState = replica
          .prepare('SELECT * FROM "_zero.replicationState"')
          .get<{stateVersion: string}>();
        expect(replicaState).toMatchObject({stateVersion: WATERMARK_REGEX});
        expectTables(replica, {['_zero.changeLog2']: []});

        // Check replica state against the upstream slot.
        const r = replicas[i];
        const slots = await upstream`
        SELECT slot_name as "slotName", confirmed_flush_lsn as lsn 
          FROM pg_replication_slots WHERE slot_name = ${r.slot}`;
        expect(slots[0]).toEqual({
          slotName: r.slot,
          lsn: fromLexiVersion(replicaState.stateVersion),
        });

        expect(eventSink.slice(0, 2)).toMatchObject([
          {
            type: 'zero/events/status/replication/v1',
            component: 'replication',
            stage: 'Initializing',
            status: 'OK',
          },
          {
            type: 'zero/events/status/replication/v1',
            component: 'replication',
            stage: 'Initializing',
            status: 'OK',
            description: /Copying \d+ upstream tables at version \w+/,
          },
        ]);
        expect(eventSink.at(-1)).toMatchObject({
          type: 'zero/events/status/replication/v1',
          component: 'replication',
          stage: 'Indexing',
          status: 'OK',
          description: /Creating \d+ indexes/,
        });
      }
    });
  }

  test('resume initial sync with invalid table', async () => {
    const lc = createSilentLogContext();
    const replica = new Database(lc, ':memory:');
    const shardConfig = {appID: APP_ID, shardNum: SHARD_NUM, publications: []};

    await ensureShardSchema(lc, upstream, shardConfig);

    // Shard should be setup to publish all "public" tables.
    // Now add an invalid table that becomes part of that publication.

    await upstream`CREATE TABLE "has/inval!d/ch@aracter$"(id int4)`;

    let result;
    try {
      await initialSync(lc, shardConfig, replica, getConnectionURI(upstream), {
        tableCopyWorkers: 1,
      });
    } catch (e) {
      result = e;
    }
    expect(result).toBeInstanceOf(UnsupportedTableSchemaError);
  });

  test.each([
    [
      'missing default publication',
      `DROP PUBLICATION "_${APP_ID}_public_${SHARD_NUM}"`,
    ],
    [
      'missing metadata publication',
      `DROP PUBLICATION "_${APP_ID}_metadata_${SHARD_NUM}"`,
    ],
    [
      'dropped schema with vestigial publications',
      `DROP SCHEMA "${APP_ID}_${SHARD_NUM}" CASCADE`,
    ],
  ])('recover from corrupted state: %s', async (_name, corruption) => {
    const lc = createSilentLogContext();
    const sql = upstream;
    await sql`CREATE TABLE foo(id int4 PRIMARY KEY);`;
    await sql`INSERT INTO foo(id) VALUES (1);`;

    const replica = new Database(lc, ':memory:');
    const shardConfig = {
      appID: APP_ID,
      shardNum: SHARD_NUM,
      publications: [],
    };

    await ensureShardSchema(lc, upstream, shardConfig);

    await sql.unsafe(corruption);

    await initialSync(lc, shardConfig, replica, getConnectionURI(upstream), {
      tableCopyWorkers: 1,
    });

    expectMatchingObjectsInTables(replica, {
      [`${APP_ID}_${SHARD_NUM}.clients`]: [],
      foo: [{id: 1}],
    });
  });

  test.each([
    'UPPERCASE',
    'dashes-not-allowed',
    'spaces not allowed',
    'punctuation!',
  ])('invalid app ID: %s', async appID => {
    const lc = createSilentLogContext();
    const replica = new Database(lc, ':memory:');
    let result;
    try {
      await initialSync(
        lc,
        {appID, shardNum: 0, publications: []},
        replica,
        getConnectionURI(upstream),
        {tableCopyWorkers: 1},
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
