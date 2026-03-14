import {beforeEach, describe, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {promiseVoid} from '../../../../../shared/src/resolved-promises.ts';
import {
  DbFile,
  expectMatchingObjectsInTables,
  initDB as initLiteDB,
} from '../../../test/lite.ts';
import {initReplicationState} from '../../replicator/schema/replication-state.ts';
import {CREATE_TABLE_METADATA_TABLE} from '../../replicator/schema/table-metadata.ts';
import {
  CREATE_V6_COLUMN_METADATA_TABLE,
  CREATE_V7_CHANGE_LOG,
  CREATE_V9_TABLE_METADATA_TABLE,
  CURRENT_SCHEMA_VERSION,
  initReplica,
} from './replica-schema.ts';

export const CURRENT_SCHEMA_VERSIONS = {
  dataVersion: CURRENT_SCHEMA_VERSION,
  schemaVersion: CURRENT_SCHEMA_VERSION,
  minSafeVersion: 1,
  lock: 1, // Internal column, always 1
};

const CREATE_VERSION_HISTORY = /*sql*/ `
  CREATE TABLE "_zero.versionHistory" (
    dataVersion INTEGER NOT NULL,
    schemaVersion INTEGER NOT NULL,
    minSafeVersion INTEGER NOT NULL,
    lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
`;

const CREATE_V1_REPLICATION_CONFIG_TABLE = /*sql*/ `
  CREATE TABLE "_zero.replicationConfig" (
    replicaVersion TEXT NOT NULL,
    publications TEXT NOT NULL,
    lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
`;

const CREATE_V11_TABLE_METADATA_TABLE = /*sql*/ `
  CREATE TABLE "_zero.tableMetadata" (
    "schema"           TEXT NOT NULL,
    "table"            TEXT NOT NULL,
    "minRowVersion"    TEXT NOT NULL DEFAULT "00",
    "upstreamMetadata" TEXT,
    PRIMARY KEY ("schema", "table")
  );
`;

describe('replica-schema-migrations', () => {
  type Case = {
    fromSchemaVersion: number;
    fromDataVersion?: number;
    desc: string;
    replicaSetup?: string;
    replicaPreState?: Record<string, object[]>;
    replicaPostState: Record<string, object[]>;
  };

  const cases: Case[] = [
    {
      fromSchemaVersion: 0,
      desc: 'start from scratch',
      replicaPostState: {
        ['_zero.replicationConfig']: [
          {
            replicaVersion: '123',
            publications: '["foo_publication"]',
            initialSyncContext: '{"context":"bar"}',
          },
        ],
      },
    },
    {
      fromSchemaVersion: 6,
      desc: 're-populate column metadata',
      replicaSetup:
        `
        CREATE TABLE users("userID" "INTEGER|NOT_NULL", password TEXT, handle TEXT);
        CREATE TABLE "_zero.changeLog" (
          old_legacy_table TEXT
        );
        ` +
        CREATE_V1_REPLICATION_CONFIG_TABLE +
        CREATE_V6_COLUMN_METADATA_TABLE,
      replicaPreState: {
        ['_zero.replicationConfig']: [
          {
            replicaVersion: '123',
            publications: '["foo_publication"]',
          },
        ],
        ['_zero.column_metadata']: [
          {
            character_max_length: null,
            column_name: 'userID',
            is_array: 0,
            is_enum: 0,
            is_not_null: 1,
            table_name: 'users',
            upstream_type: 'this should be overwritten',
          },
        ],
      },
      replicaPostState: {
        ['_zero.replicationConfig']: [
          {
            replicaVersion: '123',
            publications: '["foo_publication"]',
            initialSyncContext: '{}',
          },
        ],
        ['_zero.column_metadata']: [
          {
            character_max_length: null,
            column_name: 'userID',
            is_array: 0,
            is_enum: 0,
            is_not_null: 1,
            table_name: 'users',
            upstream_type: 'INTEGER',
            backfill: null,
          },
          {
            character_max_length: null,
            column_name: 'password',
            is_array: 0,
            is_enum: 0,
            is_not_null: 0,
            table_name: 'users',
            upstream_type: 'TEXT',
            backfill: null,
          },
          {
            character_max_length: null,
            column_name: 'handle',
            is_array: 0,
            is_enum: 0,
            is_not_null: 0,
            table_name: 'users',
            upstream_type: 'TEXT',
            backfill: null,
          },
        ],
        ['_zero.tableMetadata']: [],
      },
    },
    {
      fromSchemaVersion: 7,
      desc: 'create column metadata',
      replicaSetup:
        `
        CREATE TABLE users("userID" "INTEGER|NOT_NULL", password TEXT, handle TEXT);
      ` +
        CREATE_V1_REPLICATION_CONFIG_TABLE +
        CREATE_V6_COLUMN_METADATA_TABLE +
        CREATE_V7_CHANGE_LOG,
      replicaPostState: {
        ['_zero.column_metadata']: [
          {
            character_max_length: null,
            column_name: 'userID',
            is_array: 0,
            is_enum: 0,
            is_not_null: 1,
            table_name: 'users',
            upstream_type: 'INTEGER',
            backfill: null,
          },
          {
            character_max_length: null,
            column_name: 'password',
            is_array: 0,
            is_enum: 0,
            is_not_null: 0,
            table_name: 'users',
            upstream_type: 'TEXT',
            backfill: null,
          },
          {
            character_max_length: null,
            column_name: 'handle',
            is_array: 0,
            is_enum: 0,
            is_not_null: 0,
            table_name: 'users',
            upstream_type: 'TEXT',
            backfill: null,
          },
        ],
        ['_zero.tableMetadata']: [],
      },
    },
    {
      fromSchemaVersion: 8,
      desc: 'add backfill metadata',
      replicaSetup:
        `
        CREATE TABLE users("userID" "INTEGER|NOT_NULL", password TEXT, handle TEXT);
      ` +
        CREATE_V1_REPLICATION_CONFIG_TABLE +
        CREATE_V6_COLUMN_METADATA_TABLE +
        CREATE_V7_CHANGE_LOG,
      replicaPreState: {
        ['_zero.changeLog2']: [
          {
            stateVersion: '123',
            pos: 0,
            table: 'users',
            rowKey: '{"userID":1}',
            op: 's',
          },
        ],
        ['_zero.column_metadata']: [
          {
            character_max_length: null,
            column_name: 'userID',
            is_array: 0,
            is_enum: 0,
            is_not_null: 1,
            table_name: 'users',
            upstream_type: 'INTEGER',
          },
          {
            character_max_length: null,
            column_name: 'password',
            is_array: 0,
            is_enum: 0,
            is_not_null: 0,
            table_name: 'users',
            upstream_type: 'TEXT',
          },
          {
            character_max_length: null,
            column_name: 'handle',
            is_array: 0,
            is_enum: 0,
            is_not_null: 0,
            table_name: 'users',
            upstream_type: 'TEXT',
          },
        ],
      },
      replicaPostState: {
        ['_zero.changeLog2']: [
          {
            stateVersion: '123',
            pos: 0,
            table: 'users',
            rowKey: '{"userID":1}',
            op: 's',
            backfillingColumnVersions: '{}',
          },
        ],
        ['_zero.column_metadata']: [
          {
            character_max_length: null,
            column_name: 'userID',
            is_array: 0,
            is_enum: 0,
            is_not_null: 1,
            table_name: 'users',
            upstream_type: 'INTEGER',
            backfill: null,
          },
          {
            character_max_length: null,
            column_name: 'password',
            is_array: 0,
            is_enum: 0,
            is_not_null: 0,
            table_name: 'users',
            upstream_type: 'TEXT',
            backfill: null,
          },
          {
            character_max_length: null,
            column_name: 'handle',
            is_array: 0,
            is_enum: 0,
            is_not_null: 0,
            table_name: 'users',
            upstream_type: 'TEXT',
            backfill: null,
          },
        ],
        ['_zero.tableMetadata']: [],
      },
    },
    {
      fromSchemaVersion: 9,
      desc: 'add minRowVersion',
      replicaSetup:
        `
        CREATE TABLE users("userID" "INTEGER|NOT_NULL", password TEXT, handle TEXT);
      ` +
        CREATE_V1_REPLICATION_CONFIG_TABLE +
        CREATE_V6_COLUMN_METADATA_TABLE +
        CREATE_V7_CHANGE_LOG +
        CREATE_V9_TABLE_METADATA_TABLE,
      replicaPreState: {
        ['_zero.tableMetadata']: [
          {
            schema: 'foo',
            table: 'bar',
            metadata: '{"foo":"bar"}',
          },
        ],
      },
      replicaPostState: {
        ['_zero.tableMetadata']: [
          {
            schema: 'foo',
            table: 'bar',
            minRowVersion: '00',
            upstreamMetadata: '{"foo":"bar"}',
            metadata: null,
          },
        ],
      },
    },
    {
      fromSchemaVersion: 11,
      desc: 'restore deprecated metadata column',
      replicaSetup:
        `
        CREATE TABLE users("userID" "INTEGER|NOT_NULL", password TEXT, handle TEXT);
      ` +
        CREATE_V1_REPLICATION_CONFIG_TABLE +
        CREATE_V6_COLUMN_METADATA_TABLE +
        CREATE_V7_CHANGE_LOG +
        CREATE_V11_TABLE_METADATA_TABLE,
      replicaPreState: {
        ['_zero.tableMetadata']: [
          {
            schema: 'foo',
            table: 'bar',
            upstreamMetadata: '{"foo":"bar"}',
          },
        ],
      },
      replicaPostState: {
        ['_zero.tableMetadata']: [
          {
            schema: 'foo',
            table: 'bar',
            minRowVersion: '00',
            upstreamMetadata: '{"foo":"bar"}',
            metadata: null,
          },
        ],
      },
    },
    {
      fromSchemaVersion: 12,
      fromDataVersion: 9,
      desc: 'migrate metadata from rollback/rollforward',
      replicaSetup:
        `
        CREATE TABLE users("userID" "INTEGER|NOT_NULL", password TEXT, handle TEXT);
      ` +
        CREATE_V1_REPLICATION_CONFIG_TABLE +
        CREATE_V6_COLUMN_METADATA_TABLE +
        CREATE_V7_CHANGE_LOG +
        CREATE_TABLE_METADATA_TABLE,
      replicaPreState: {
        ['_zero.tableMetadata']: [
          {
            schema: 'foo',
            table: 'bar',
            metadata: '{"foo":"bar"}',
          },
        ],
      },
      replicaPostState: {
        ['_zero.tableMetadata']: [
          {
            schema: 'foo',
            table: 'bar',
            minRowVersion: '00',
            upstreamMetadata: '{"foo":"bar"}',
            metadata: null,
          },
        ],
      },
    },
  ];

  let replicaFile: DbFile;

  beforeEach(() => {
    replicaFile = new DbFile('replica_schema_test');
    return () => replicaFile.delete();
  });

  const lc = createSilentLogContext();

  for (const c of cases) {
    test(`from v${c.fromSchemaVersion}: ${c.desc}`, async () => {
      const replica = replicaFile.connect(lc);
      initLiteDB(replica, (c.replicaSetup ?? '') + CREATE_VERSION_HISTORY, {
        ['_zero.versionHistory']: [
          {
            schemaVersion: c.fromSchemaVersion,
            dataVersion: c.fromDataVersion ?? c.fromSchemaVersion,
            minSafeVersion: 1,
          },
        ],
        ...c.replicaPreState,
      });

      await initReplica(lc, 'test', replicaFile.path, (_, db) => {
        initReplicationState(db, ['foo_publication'], '123', {context: 'bar'});
        return promiseVoid;
      });

      expectMatchingObjectsInTables(replica, {
        ['_zero.versionHistory']: [CURRENT_SCHEMA_VERSIONS],
        ...c.replicaPostState,
      });
    });
  }
});
