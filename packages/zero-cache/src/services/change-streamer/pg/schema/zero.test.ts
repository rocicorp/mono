import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../../shared/src/logging-test-utils.js';
import {initDB, testDBs} from '../../../../test/db.js';
import type {PostgresDB} from '../../../../types/pg.js';
import {setupTablesAndReplication} from './zero.js';

describe('change-source/pg', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;

  beforeEach(async () => {
    db = await testDBs.create('zero_schema_test');
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  function publications() {
    return db<{pubname: string; rowfilter: string | null}[]>`
    SELECT p.pubname, rowfilter FROM pg_publication p
      LEFT JOIN pg_publication_tables t ON p.pubname = t.pubname 
      WHERE p.pubname LIKE '%zero_%' ORDER BY p.pubname`.values();
  }

  test('default publication, schema version setup', async () => {
    // Run twice. Repeat should be a no-op.
    for (let i = 0; i < 2; i++) {
      await db.begin(tx =>
        setupTablesAndReplication(lc, tx, {id: '0', publications: []}),
      );

      expect(await publications()).toEqual([
        ['_zero_0_clients', `("shardID" = '0'::text)`],
        ['_zero_schema_versions', null],
        ['zero_public', null],
      ]);

      expect(
        await db`SELECT "minSupportedVersion", "maxSupportedVersion" FROM zero."schemaVersions"`.values(),
      ).toEqual([[1, 1]]);
    }
  });

  test('weird shard IDs', async () => {
    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {id: `'has quotes'`, publications: []}),
    );

    expect(await publications()).toEqual([
      [`_zero_'has quotes'_clients`, `("shardID" = '''has quotes'''::text)`],
      ['_zero_schema_versions', null],
      ['zero_public', null],
    ]);
  });

  test('multiple shards', async () => {
    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {id: '0', publications: []}),
    );
    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {id: '1', publications: []}),
    );

    expect(await publications()).toEqual([
      ['_zero_0_clients', `("shardID" = '0'::text)`],
      ['_zero_1_clients', `("shardID" = '1'::text)`],
      ['_zero_schema_versions', null],
      ['zero_public', null],
    ]);
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
      ['_zero_A_clients', `("shardID" = 'A'::text)`],
      ['_zero_schema_versions', null],
      ['zero_bar', null],
      ['zero_foo', '(id > 1000)'],
    ]);
  });

  type InvalidUpstreamCase = {
    error: string;
    setupUpstreamQuery?: string;
    requestedPublications?: string[];
    upstream?: Record<string, object[]>;
  };

  const invalidUpstreamCases: InvalidUpstreamCase[] = [
    {
      error: 'does not have a PRIMARY KEY',
      setupUpstreamQuery: `
        CREATE TABLE issues("issueID" INTEGER, "orgID" INTEGER);
      `,
    },
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
    test(`Invalid upstream: ${c.error}`, async () => {
      await initDB(db, c.setupUpstreamQuery, c.upstream);

      const result = await db
        .begin(tx =>
          setupTablesAndReplication(lc, tx, {
            id: SHARD_ID,
            publications: c.requestedPublications ?? [],
          }),
        )
        .catch(e => e);

      expect(result).toBeInstanceOf(Error);
      expect(String(result)).toContain(c.error);
    });
  }
});
