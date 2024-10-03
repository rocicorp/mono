import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from 'zero-cache/src/test/db.js';
import type {PostgresDB} from 'zero-cache/src/types/pg.js';
import {setupTablesAndReplication} from './zero.js';

describe('change-source/pg', () => {
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
        setupTablesAndReplication(tx, {id: '0', publications: []}),
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
      setupTablesAndReplication(tx, {id: `'has quotes'`, publications: []}),
    );

    expect(await publications()).toEqual([
      [`_zero_'has quotes'_clients`, `("shardID" = '''has quotes'''::text)`],
      ['_zero_schema_versions', null],
      ['zero_public', null],
    ]);
  });

  test('multiple shards', async () => {
    await db.begin(tx =>
      setupTablesAndReplication(tx, {id: '0', publications: []}),
    );
    await db.begin(tx =>
      setupTablesAndReplication(tx, {id: '1', publications: []}),
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
        setupTablesAndReplication(tx, {
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
    CREATE TABLE foo(id INT4);
    CREATE TABLE bar(id TEXT);
    CREATE PUBLICATION zero_foo FOR TABLE foo WHERE (id > 1000);
    CREATE PUBLICATION zero_bar FOR TABLE bar;`.simple();

    await db.begin(tx =>
      setupTablesAndReplication(tx, {
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
});
