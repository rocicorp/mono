import {beforeEach, describe, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {
  DbFile,
  expectMatchingObjectsInTables,
  initDB as initLiteDB,
} from '../../test/lite.ts';
import {CREATE_COLUMN_METADATA_TABLE} from '../replicator/schema/column-metadata.ts';
import {initReplica} from './replica-schema.ts';

// Update as necessary.
const CURRENT_SCHEMA_VERSIONS = {
  dataVersion: 8,
  schemaVersion: 8,
  minSafeVersion: 1,
  lock: 1, // Internal column, always 1
};

describe('replica-schema-migrations', () => {
  type Case = {
    name: string;

    replicaSetup?: string;
    replicaPreState?: Record<string, object[]>;
    replicaPostState: Record<string, object[]>;
  };

  const cases: Case[] = [
    {
      name: 'v6 to v8: re-populate column metadata',
      replicaSetup:
        `
        CREATE TABLE users("userID" "INTEGER|NOT_NULL", password TEXT, handle TEXT);
        CREATE TABLE "_zero.changeLog" (
          old_legacy_table TEXT
        );
        CREATE TABLE "_zero.versionHistory" (
          dataVersion INTEGER NOT NULL,
          schemaVersion INTEGER NOT NULL,
          minSafeVersion INTEGER NOT NULL,
          lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
        );
    ` + CREATE_COLUMN_METADATA_TABLE,
      replicaPreState: {
        ['_zero.versionHistory']: [
          {
            dataVersion: 6,
            schemaVersion: 6,
            minSafeVersion: 1,
            lock: 1, // Internal column, always 1
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
        ['_zero.versionHistory']: [CURRENT_SCHEMA_VERSIONS],
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
    },
    {
      name: 'v7 to v8: create column metadata',
      replicaSetup: `
        CREATE TABLE users("userID" "INTEGER|NOT_NULL", password TEXT, handle TEXT);
        CREATE TABLE "_zero.changeLog" (
          old_legacy_table TEXT
        );
        CREATE TABLE "_zero.versionHistory" (
          dataVersion INTEGER NOT NULL,
          schemaVersion INTEGER NOT NULL,
          minSafeVersion INTEGER NOT NULL,
          lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
        );
        `,
      replicaPreState: {
        ['_zero.versionHistory']: [
          {
            dataVersion: 7,
            schemaVersion: 7,
            minSafeVersion: 1,
            lock: 1, // Internal column, always 1
          },
        ],
      },
      replicaPostState: {
        ['_zero.versionHistory']: [CURRENT_SCHEMA_VERSIONS],
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
    },
  ];

  let replicaFile: DbFile;

  beforeEach(() => {
    replicaFile = new DbFile('replica_schema_test');
    return () => replicaFile.delete();
  });

  const lc = createSilentLogContext();

  for (const c of cases) {
    test(c.name, async () => {
      const replica = replicaFile.connect(lc);
      initLiteDB(replica, c.replicaSetup, c.replicaPreState);

      await initReplica(lc, 'test', replicaFile.path, async () => {});

      expectMatchingObjectsInTables(replica, c.replicaPostState);
    });
  }
});
