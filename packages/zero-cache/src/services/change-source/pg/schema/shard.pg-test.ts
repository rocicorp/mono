import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {TestLogSink} from '../../../../../../shared/src/logging-test-utils.ts';
import {Index} from '../../../../db/postgres-replica-identity-enum.ts';
import {expectTables, initDB, testDBs} from '../../../../test/db.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import {getPublicationInfo} from './published.ts';
import {setupTablesAndReplication, validatePublications} from './shard.ts';

describe('change-source/pg', () => {
  let logSink: TestLogSink;
  let lc: LogContext;
  let db: PostgresDB;

  beforeEach(async () => {
    logSink = new TestLogSink();
    lc = new LogContext('warn', {}, logSink);
    db = await testDBs.create('zero_schema_test');
  });

  afterEach(async () => {
    await testDBs.drop(db);
    await testDBs.sql`RESET ROLE; DROP ROLE IF EXISTS supaneon`.simple();
  });

  function publications() {
    return db<{pubname: string; rowfilter: string | null}[]>`
    SELECT p.pubname, t.schemaname, t.tablename, rowfilter FROM pg_publication p
      LEFT JOIN pg_publication_tables t ON p.pubname = t.pubname 
      WHERE p.pubname LIKE '%zero_%' ORDER BY p.pubname`.values();
  }

  test('default publication, schema version setup', async () => {
    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {id: '0', publications: []}),
    );

    expect(await publications()).toEqual([
      [`_zero_metadata_0`, 'zero', 'schemaVersions', null],
      [`_zero_metadata_0`, `zero_0`, 'clients', null],
      ['_zero_public_0', null, null, null],
    ]);

    await expectTables(db, {
      ['zero.schemaVersions']: [
        {lock: true, minSupportedVersion: 1, maxSupportedVersion: 1},
      ],
      ['zero_0.shardConfig']: [
        {
          lock: true,
          publications: ['_zero_metadata_0', '_zero_public_0'],
          ddlDetection: true,
          initialSchema: null,
        },
      ],
      ['zero_0.clients']: [],
    });

    expect(
      (await db`SELECT evtname from pg_event_trigger`.values()).flat(),
    ).toEqual([
      'zero_ddl_start_0',
      'zero_create_table_0',
      'zero_alter_table_0',
      'zero_create_index_0',
      'zero_drop_table_0',
      'zero_drop_index_0',
      'zero_alter_publication_0',
    ]);
  });

  test('default publication, join table', async () => {
    await db.unsafe(`
    CREATE TABLE join_table(id1 TEXT NOT NULL, id2 TEXT NOT NULL);
    CREATE UNIQUE INDEX join_key ON join_table (id1, id2);
    INSERT INTO join_table (id1, id2) VALUES ('foo', 'bar');
    `);

    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {id: '0', publications: []}),
    );

    expect(await publications()).toEqual([
      [`_zero_metadata_0`, 'zero', 'schemaVersions', null],
      [`_zero_metadata_0`, `zero_0`, 'clients', null],
      ['_zero_public_0', 'public', 'join_table', null],
    ]);

    await expectTables(db, {
      ['zero.schemaVersions']: [
        {lock: true, minSupportedVersion: 1, maxSupportedVersion: 1},
      ],
      ['zero_0.shardConfig']: [
        {
          lock: true,
          publications: ['_zero_metadata_0', '_zero_public_0'],
          ddlDetection: true,
          initialSchema: null,
        },
      ],
      ['zero_0.clients']: [],
      ['join_table']: [{id1: 'foo', id2: 'bar'}],
    });

    const pubs = await getPublicationInfo(db, ['_zero_public_0']);
    const table = pubs.tables.find(t => t.name === 'join_table');
    expect(table?.replicaIdentity).toBe(Index);

    const index = pubs.indexes.find(idx => idx.name === 'join_key');
    expect(index?.isReplicaIdentity).toBe(true);
  });

  test('weird shard IDs', async () => {
    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {id: `'has quotes'`, publications: []}),
    );

    expect(await publications()).toEqual([
      [`_zero_metadata_'has quotes'`, 'zero', 'schemaVersions', null],
      [`_zero_metadata_'has quotes'`, `zero_'has quotes'`, 'clients', null],
      [`_zero_public_'has quotes'`, null, null, null],
    ]);

    await expectTables(db, {
      ['zero.schemaVersions']: [
        {lock: true, minSupportedVersion: 1, maxSupportedVersion: 1},
      ],
      [`zero_'has quotes'.shardConfig`]: [
        {
          lock: true,
          publications: [
            `_zero_metadata_'has quotes'`,
            `_zero_public_'has quotes'`,
          ],
          ddlDetection: true,
          initialSchema: null,
        },
      ],
      [`zero_'has quotes'.clients`]: [],
    });
  });

  test('multiple shards', async () => {
    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {id: '0', publications: []}),
    );
    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {id: '1', publications: []}),
    );

    expect(await publications()).toEqual([
      [`_zero_metadata_0`, 'zero', 'schemaVersions', null],
      [`_zero_metadata_0`, `zero_0`, 'clients', null],
      [`_zero_metadata_1`, 'zero', 'schemaVersions', null],
      [`_zero_metadata_1`, `zero_1`, 'clients', null],
      ['_zero_public_0', null, null, null],
      ['_zero_public_1', null, null, null],
    ]);

    await expectTables(db, {
      ['zero.schemaVersions']: [
        {lock: true, minSupportedVersion: 1, maxSupportedVersion: 1},
      ],
      ['zero_0.shardConfig']: [
        {
          lock: true,
          publications: ['_zero_metadata_0', '_zero_public_0'],
          ddlDetection: true,
          initialSchema: null,
        },
      ],
      ['zero_0.clients']: [],
      ['zero_1.shardConfig']: [
        {
          lock: true,
          publications: ['_zero_metadata_1', '_zero_public_1'],
          ddlDetection: true,
          initialSchema: null,
        },
      ],
      ['zero_1.clients']: [],
    });
  });

  test('unknown publications', async () => {
    let err;
    try {
      await db.begin(tx =>
        setupTablesAndReplication(lc, tx, {
          id: '0',
          publications: ['zero_invalid'],
        }),
      );
    } catch (e) {
      err = e;
    }
    expect(err).toMatchInlineSnapshot(
      `[Error: Unknown or invalid publications. Specified: [zero_invalid]. Found: []]`,
    );

    expect(await publications()).toEqual([]);
  });

  test('supplied publications', async () => {
    await db`
    CREATE TABLE foo(id INT4 PRIMARY KEY);
    CREATE TABLE bar(id TEXT PRIMARY KEY);
    CREATE PUBLICATION zero_foo FOR TABLE foo WHERE (id > 1000);
    CREATE PUBLICATION zero_bar FOR TABLE bar;`.simple();

    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {
        id: 'A',
        publications: ['zero_foo', 'zero_bar'],
      }),
    );

    expect(await publications()).toEqual([
      [`_zero_metadata_A`, 'zero', 'schemaVersions', null],
      [`_zero_metadata_A`, `zero_A`, 'clients', null],
      ['zero_bar', 'public', 'bar', null],
      ['zero_foo', 'public', 'foo', '(id > 1000)'],
    ]);

    await expectTables(db, {
      ['zero.schemaVersions']: [
        {lock: true, minSupportedVersion: 1, maxSupportedVersion: 1},
      ],
      ['zero_A.shardConfig']: [
        {
          lock: true,
          publications: ['_zero_metadata_A', 'zero_bar', 'zero_foo'],
          ddlDetection: true,
          initialSchema: null,
        },
      ],
      ['zero_A.clients']: [],
    });
  });

  test('non-superuser: ddlDetection = false', async () => {
    await db`
    CREATE TABLE foo(id INT4 PRIMARY KEY);
    CREATE PUBLICATION zero_foo FOR TABLE foo;
    
    CREATE ROLE supaneon NOSUPERUSER IN ROLE current_user;
    SET ROLE supaneon;
    `.simple();

    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {
        id: 'supaneon',
        publications: ['zero_foo'],
      }),
    );

    expect(await publications()).toEqual([
      [`_zero_metadata_supaneon`, 'zero', 'schemaVersions', null],
      [`_zero_metadata_supaneon`, `zero_supaneon`, 'clients', null],
      ['zero_foo', 'public', 'foo', null],
    ]);

    await expectTables(db, {
      ['zero.schemaVersions']: [
        {lock: true, minSupportedVersion: 1, maxSupportedVersion: 1},
      ],
      ['zero_supaneon.shardConfig']: [
        {
          lock: true,
          publications: ['_zero_metadata_supaneon', 'zero_foo'],
          ddlDetection: false, // degraded mode
          initialSchema: null,
        },
      ],
      ['zero_supaneon.clients']: [],
    });

    expect(logSink.messages[0]).toMatchInlineSnapshot(`
      [
        "warn",
        {},
        [
          "Unable to create event triggers for schema change detection:

      "Must be superuser to create an event trigger."

      Proceeding in degraded mode: schema changes will halt replication,
      requiring the replica to be reset (manually or with --auto-reset).",
        ],
      ]
    `);

    expect(await db`SELECT evtname from pg_event_trigger`.values()).toEqual([]);
  });

  type InvalidUpstreamCase = {
    error: string;
    setupUpstreamQuery?: string;
    requestedPublications?: string[];
    upstream?: Record<string, object[]>;
  };

  const invalidUpstreamCases: InvalidUpstreamCase[] = [
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
      error: 'Only the default "public" schema is supported',
      setupUpstreamQuery: `
        CREATE SCHEMA _zero;
        CREATE TABLE _zero.is_not_allowed(
          "issueID" INTEGER PRIMARY KEY, 
          "orgID" INTEGER
        );
        CREATE PUBLICATION zero_foo FOR TABLES IN SCHEMA _zero;
        `,
      requestedPublications: ['zero_foo'],
    },
    {
      error: 'Only the default "public" schema is supported',
      setupUpstreamQuery: `
        CREATE SCHEMA unsupported;
        CREATE TABLE unsupported.issues ("issueID" INTEGER PRIMARY KEY, "orgID" INTEGER);
        CREATE PUBLICATION zero_foo FOR TABLES IN SCHEMA unsupported;
      `,
      requestedPublications: ['zero_foo'],
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

  const SHARD_ID = 'publication_validation_test_id';

  for (const c of invalidUpstreamCases) {
    test(`Invalid publication: ${c.error}`, async () => {
      await initDB(
        db,
        c.setupUpstreamQuery +
          `CREATE PUBLICATION zero_data FOR TABLES IN SCHEMA public;`,
        c.upstream,
      );

      const published = await getPublicationInfo(db, [
        'zero_data',
        ...(c.requestedPublications ?? []),
      ]);
      expect(() => validatePublications(lc, SHARD_ID, published)).toThrowError(
        c.error,
      );
    });
  }
});
