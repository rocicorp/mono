import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {promiseVoid} from '../../../../../shared/src/resolved-promises.ts';
import {runSchemaMigrations} from '../../../db/migration-lite.ts';
import {
  DbFile,
  expectMatchingObjectsInTables,
  initDB as initLiteDB,
} from '../../../test/lite.ts';
import {BACKFILLING_TABLE} from '../../replicator/schema/backfilling.ts';
import {CREATE_COLUMN_METADATA_TABLE} from '../../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../../replicator/schema/replication-state.ts';
import {CREATE_TABLE_METADATA_TABLE} from '../../replicator/schema/table-metadata.ts';
import {
  CREATE_V6_COLUMN_METADATA_TABLE,
  CREATE_V7_CHANGE_LOG,
  CREATE_V9_TABLE_METADATA_TABLE,
  CREATE_V14_CHANGE_LOG_STREAM,
  CURRENT_SCHEMA_VERSION,
  initReplica,
  schemaVersionMigrationMap,
  V14_CHANGE_LOG_STREAM_TABLE,
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
  CREATE TABLE "_zero.replicationState" (
    stateVersion TEXT NOT NULL,
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

// `_zero.replicationState` as of v15, i.e. with writeTimeMs NOT NULL.
const CREATE_V15_REPLICATION_STATE_TABLE = /*sql*/ `
  CREATE TABLE "_zero.replicationState" (
    stateVersion TEXT NOT NULL,
    writeTimeMs INTEGER NOT NULL,
    lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
`;

const V15_REPLICATION_STATE = [
  {stateVersion: '123', writeTimeMs: 12345, lock: 1},
];

function columnMetadata(
  tableName: string,
  columnName: string,
  backfill: string | null,
) {
  return {
    table_name: tableName,
    column_name: columnName,
    upstream_type: 'text',
    is_not_null: 0,
    is_enum: 0,
    is_array: 0,
    character_max_length: null,
    backfill,
  };
}

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
        CREATE_COLUMN_METADATA_TABLE +
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
        CREATE_COLUMN_METADATA_TABLE +
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
        CREATE_COLUMN_METADATA_TABLE +
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
    {
      fromSchemaVersion: 12,
      fromDataVersion: 12,
      desc: 'backfill writeTimeMs column',
      replicaSetup:
        CREATE_V1_REPLICATION_CONFIG_TABLE +
        CREATE_COLUMN_METADATA_TABLE +
        CREATE_V7_CHANGE_LOG +
        CREATE_TABLE_METADATA_TABLE,
      replicaPreState: {
        ['_zero.replicationState']: [
          {
            stateVersion: '123',
            lock: 1,
          },
        ],
      },
      replicaPostState: {
        ['_zero.replicationState']: [
          {
            stateVersion: '123',
            writeTimeMs: expect.any(Number),
            lock: 1,
          },
        ],
      },
    },
    {
      // v14 creates and seeds the change-log stream table; v16 drops it. Both
      // run here, in that order.
      fromSchemaVersion: 13,
      fromDataVersion: 11,
      desc: 'preserves writeTimeMs after rollback and rollforward',
      replicaSetup:
        /*sql*/ `CREATE TABLE "_zero.replicationState" (
          stateVersion TEXT NOT NULL,
          writeTimeMs INTEGER,
          lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
        );` +
        CREATE_COLUMN_METADATA_TABLE +
        CREATE_V7_CHANGE_LOG +
        CREATE_TABLE_METADATA_TABLE,
      replicaPreState: {
        ['_zero.replicationState']: [
          {
            stateVersion: '123',
            writeTimeMs: 12345,
            lock: 1,
          },
        ],
      },
      replicaPostState: {
        ['_zero.replicationState']: [
          {
            stateVersion: '123',
            writeTimeMs: 12345,
            lock: 1,
          },
        ],
      },
    },
    {
      // A replica that a v14 zero-cache created, seed included. v16 drops the
      // table out from under it.
      fromSchemaVersion: 14,
      fromDataVersion: 14,
      desc: 'drops the change-log stream table a v14 zero-cache created',
      replicaSetup:
        /*sql*/ `CREATE TABLE "_zero.replicationState" (
          stateVersion TEXT NOT NULL,
          writeTimeMs INTEGER,
          lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
        );` +
        CREATE_COLUMN_METADATA_TABLE +
        CREATE_TABLE_METADATA_TABLE +
        CREATE_V14_CHANGE_LOG_STREAM,
      replicaPreState: {
        ['_zero.replicationState']: [
          {
            stateVersion: '123',
            writeTimeMs: 12345,
            lock: 1,
          },
        ],
        ['_zero.changeLogStream']: [
          {
            watermark: '123',
            pos: 0,
            change: '{"tag":"begin"}',
            precommit: null,
            writeTimeMs: null,
          },
          {
            watermark: '123',
            pos: 1,
            change: '{"tag":"commit"}',
            precommit: '123',
            writeTimeMs: 12345,
          },
        ],
      },
      replicaPostState: {
        ['_zero.replicationState']: [
          {
            stateVersion: '123',
            writeTimeMs: 12345,
            lock: 1,
          },
        ],
      },
    },
    {
      // Rolled back to v14 code, which reset dataVersion to 14 while leaving
      // schemaVersion at 16, then rolled forward. Migration 16 re-runs with
      // its schema step skipped, so it must not resurrect the table.
      fromSchemaVersion: 16,
      fromDataVersion: 14,
      desc: 'stays dropped after rollback and rollforward',
      // writeTimeMs is NOT NULL because a v16 schema has been through v15.
      replicaSetup:
        CREATE_V15_REPLICATION_STATE_TABLE +
        CREATE_COLUMN_METADATA_TABLE +
        CREATE_TABLE_METADATA_TABLE,
      replicaPreState: {
        ['_zero.replicationState']: [
          {
            stateVersion: '123',
            writeTimeMs: 12345,
            lock: 1,
          },
        ],
      },
      replicaPostState: {
        ['_zero.replicationState']: [
          {
            stateVersion: '123',
            writeTimeMs: 12345,
            lock: 1,
          },
        ],
      },
    },
    {
      // The identity that `_zero.column_metadata` cannot supply: it is keyed by
      // lite table name, which collapses the `public` schema and has no
      // inverse. A metadata row resolves the schema exactly; a name with no dot
      // in it is exactly `public`; anything else is a best-effort split.
      fromSchemaVersion: 16,
      desc: 'seeds the backfilling table from column metadata',
      replicaSetup:
        CREATE_V15_REPLICATION_STATE_TABLE +
        CREATE_COLUMN_METADATA_TABLE +
        CREATE_TABLE_METADATA_TABLE,
      replicaPreState: {
        ['_zero.replicationState']: V15_REPLICATION_STATE,
        ['_zero.tableMetadata']: [
          {
            schema: 'my',
            table: 'foo',
            minRowVersion: '00',
            upstreamMetadata: '{"rowKey":{"type":"default","columns":["id"]}}',
            metadata: null,
          },
        ],
        ['_zero.column_metadata']: [
          columnMetadata('my.foo', 'a', '{"fooID":1}'),
          columnMetadata('my.foo', 'b', null), // not backfilling
          columnMetadata('bar', 'c', '{"fooID":2}'),
          columnMetadata('your.baz', 'd', '{"fooID":3}'), // unresolvable
        ],
      },
      replicaPostState: {
        [BACKFILLING_TABLE]: [
          {
            schema: 'public',
            table: 'bar',
            column: 'c',
            backfill: '{"fooID":2}',
          },
          {schema: 'my', table: 'foo', column: 'a', backfill: '{"fooID":1}'},
          {
            schema: 'your',
            table: 'baz',
            column: 'd',
            backfill: '{"fooID":3}',
          },
        ],
      },
    },
    {
      // Rolled back to v16 code, which does not know the table exists and
      // therefore maintains only `column_metadata.backfill` while it runs, then
      // rolled forward. Migration 17 re-runs with its schema step skipped, so
      // `migrateData` has to rebuild the table rather than merge into it.
      fromSchemaVersion: 17,
      fromDataVersion: 16,
      desc: 're-seeds the backfilling table after rollback and rollforward',
      replicaSetup:
        CREATE_V15_REPLICATION_STATE_TABLE +
        CREATE_COLUMN_METADATA_TABLE +
        CREATE_TABLE_METADATA_TABLE +
        /*sql*/ `CREATE TABLE "${BACKFILLING_TABLE}" (
          "schema"   TEXT NOT NULL,
          "table"    TEXT NOT NULL,
          "column"   TEXT NOT NULL,
          "backfill" TEXT NOT NULL,
          PRIMARY KEY ("schema", "table", "column")
        );`,
      replicaPreState: {
        ['_zero.replicationState']: V15_REPLICATION_STATE,
        ['_zero.column_metadata']: [
          columnMetadata('foo', 'a', null), // completed while rolled back
          columnMetadata('foo', 'z', '{"fooID":9}'), // started while rolled back
        ],
        [BACKFILLING_TABLE]: [
          {
            schema: 'public',
            table: 'foo',
            column: 'a',
            backfill: '{"fooID":1}',
          },
        ],
      },
      replicaPostState: {
        [BACKFILLING_TABLE]: [
          {
            schema: 'public',
            table: 'foo',
            column: 'z',
            backfill: '{"fooID":9}',
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
        ...(c.fromSchemaVersion === 0 ||
        c.replicaPreState?.['_zero.replicationState']
          ? {}
          : {
              ['_zero.replicationState']: [
                {
                  stateVersion: '123',
                },
              ],
            }),
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

      // No replica at CURRENT_SCHEMA_VERSION carries the change-log stream
      // table, by any route into it: a fresh sync never creates it, and every
      // incremental path runs migration 16.
      expect(
        replica
          .prepare(/*sql*/ `SELECT "name" FROM "sqlite_master"
                       WHERE "tbl_name" = ?`)
          .all(V14_CHANGE_LOG_STREAM_TABLE),
      ).toEqual([]);

      // Conversely, every replica at CURRENT_SCHEMA_VERSION carries the
      // backfilling table: a fresh sync creates it with the rest of the
      // replication state, and every incremental path runs migration 17.
      expect(
        replica
          .prepare(/*sql*/ `SELECT "name" FROM "sqlite_master"
                       WHERE "type" = 'table' AND "tbl_name" = ?`)
          .all(BACKFILLING_TABLE),
      ).toEqual([{name: BACKFILLING_TABLE}]);
    });
  }

  // Migration 17 sets no `minSafeVersion`, which is what makes this legal, and
  // the reason it can: nothing at v16 reads "_zero.backfilling", and what a v16
  // zero-cache does write — "_zero.column_metadata.backfill" — is what
  // migration 17's `migrateData` rebuilds the table from on the way forward.
  test('a v16 zero-cache runs against a v17 replica', async () => {
    const replica = replicaFile.connect(lc);
    initLiteDB(replica, CREATE_VERSION_HISTORY, {});
    await initReplica(lc, 'test', replicaFile.path, (_, db) => {
      initReplicationState(db, ['foo_publication'], '123');
      return promiseVoid;
    });
    replica
      .prepare(/*sql*/ `INSERT INTO "${BACKFILLING_TABLE}"
                 ("schema", "table", "column", "backfill") VALUES (?, ?, ?, ?)`)
      .run('my', 'foo', 'a', '{"fooID":1}');

    const v16MigrationMap = Object.fromEntries(
      Object.entries(schemaVersionMigrationMap).filter(
        ([version]) => Number(version) <= 16,
      ),
    );
    await runSchemaMigrations(
      lc,
      'test',
      replicaFile.path,
      {
        migrateSchema: () => {
          throw new Error('the replica is already synced');
        },
      },
      v16MigrationMap,
    );

    expectMatchingObjectsInTables(replica, {
      // The data version rolls back; the schema version never moves backwards.
      ['_zero.versionHistory']: [
        {dataVersion: 16, schemaVersion: 17, minSafeVersion: 1},
      ],
      // The table is left alone rather than dropped, so rolling forward does
      // not have to recreate it.
      [BACKFILLING_TABLE]: [
        {schema: 'my', table: 'foo', column: 'a', backfill: '{"fooID":1}'},
      ],
    });
  });
});
