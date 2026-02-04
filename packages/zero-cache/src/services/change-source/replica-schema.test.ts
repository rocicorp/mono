import {beforeEach, describe, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {
  DbFile,
  expectMatchingObjectsInTables,
  initDB as initLiteDB,
} from '../../test/lite.ts';
import {
  CREATE_V6_COLUMN_METADATA_TABLE,
  CREATE_V7_CHANGE_LOG,
  initReplica,
} from './replica-schema.ts';

// Update as necessary.
const CURRENT_SCHEMA_VERSIONS = {
  dataVersion: 9,
  schemaVersion: 9,
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

describe('replica-schema-migrations', () => {
  type Case = {
    fromVersion: number;
    desc: string;
    replicaSetup?: string;
    replicaPreState?: Record<string, object[]>;
    replicaPostState: Record<string, object[]>;
  };

  const cases: Case[] = [
    {
      fromVersion: 6,
      desc: 're-populate column metadata',
      replicaSetup:
        `
        CREATE TABLE users("userID" "INTEGER|NOT_NULL", password TEXT, handle TEXT);
        CREATE TABLE "_zero.changeLog" (
          old_legacy_table TEXT
        );
        ` + CREATE_V6_COLUMN_METADATA_TABLE,
      replicaPreState: {
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
      fromVersion: 7,
      desc: 'create column metadata',
      replicaSetup:
        `
        CREATE TABLE users("userID" "INTEGER|NOT_NULL", password TEXT, handle TEXT);
      ` +
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
      fromVersion: 8,
      desc: 'add backfill metadata',
      replicaSetup:
        `
        CREATE TABLE users("userID" "INTEGER|NOT_NULL", password TEXT, handle TEXT);
      ` +
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
  ];

  let replicaFile: DbFile;

  beforeEach(() => {
    replicaFile = new DbFile('replica_schema_test');
    return () => replicaFile.delete();
  });

  const lc = createSilentLogContext();

  for (const c of cases) {
    test(`from v${c.fromVersion}: ${c.desc}`, async () => {
      const replica = replicaFile.connect(lc);
      initLiteDB(replica, c.replicaSetup + CREATE_VERSION_HISTORY, {
        ['_zero.versionHistory']: [
          {
            dataVersion: c.fromVersion,
            schemaVersion: c.fromVersion,
            minSafeVersion: 1,
          },
        ],
        ...c.replicaPreState,
      });

      await initReplica(lc, 'test', replicaFile.path, async () => {});

      expectMatchingObjectsInTables(replica, {
        ['_zero.versionHistory']: [CURRENT_SCHEMA_VERSIONS],
        ...c.replicaPostState,
      });
    });
  }
});
