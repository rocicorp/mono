import pg from 'pg';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {expectTables, testDBs} from '../test/db.js';
import type {PostgresDB} from '../types/pg.js';
import type {RowKey} from '../types/row-key.js';
import {
  lookupRowsWithKeys,
  multiInsertParams,
  multiInsertStatement,
} from './queries.js';

const {
  types: {builtins},
} = pg;

type MyRow = {
  foo: number;
  bar: number;
  baz: string;
  camelCase: number;
};

test('multiInsertStatement', () => {
  expect(
    multiInsertStatement<MyRow>('cvr', 'rows', ['foo', 'bar', 'baz'], 1),
  ).toMatchInlineSnapshot(
    `"INSERT INTO cvr.rows (foo,bar,baz) VALUES ($1,$2,$3)"`,
  );
  expect(
    multiInsertStatement<MyRow>('cvr', 'rows', ['foo', 'bar', 'baz'], 2),
  ).toMatchInlineSnapshot(
    `"INSERT INTO cvr.rows (foo,bar,baz) VALUES ($1,$2,$3),($4,$5,$6)"`,
  );
  expect(
    multiInsertStatement<MyRow>(
      'cvr',
      'rows',
      ['foo', 'bar', 'baz'],
      3,
      'ON CONFLICT DO NOTHING',
    ),
  ).toMatchInlineSnapshot(
    `"INSERT INTO cvr.rows (foo,bar,baz) VALUES ($1,$2,$3),($4,$5,$6),($7,$8,$9) ON CONFLICT DO NOTHING"`,
  );
  expect(
    multiInsertStatement<MyRow>(
      'mySchema',
      'rowTable',
      ['bar', 'baz', 'camelCase'],
      3,
      'ON CONFLICT DO NOTHING',
    ),
  ).toMatchInlineSnapshot(
    `"INSERT INTO "mySchema"."rowTable" (bar,baz,"camelCase") VALUES ($1,$2,$3),($4,$5,$6),($7,$8,$9) ON CONFLICT DO NOTHING"`,
  );
});

test('multiInsertValues', () => {
  expect(
    multiInsertParams<MyRow>(
      ['camelCase', 'baz', 'foo'],
      [
        {foo: 1, bar: 2, baz: 'three', camelCase: 4},
        {foo: 10, bar: 20, baz: 'thirty', camelCase: 40},
        {foo: 15, bar: 25, baz: 'thirty-five', camelCase: 45},
      ],
    ),
  ).toEqual([4, 'three', 1, 40, 'thirty', 10, 45, 'thirty-five', 15]);
});

describe('db/queries', () => {
  let db: PostgresDB;

  beforeEach(async () => {
    db = await testDBs.create('db_queries_test');
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  test('lookupRowsWithKeys', async () => {
    await db.unsafe(`
    CREATE TABLE foo (
      id int,
      str text,
      val text,
      PRIMARY KEY(id, str)
    );
    INSERT INTO foo (id, str, val) VALUES (1, 'one', 'foo');
    INSERT INTO foo (id, str, val) VALUES (2, 'two', 'bar');
    INSERT INTO foo (id, str, val) VALUES (3, 'three', 'bonk');
    INSERT INTO foo (id, str, val) VALUES (4, 'four', 'boo');
    INSERT INTO foo (id, str, val) VALUES (5, 'five', 'boom');
    `);

    const rowKeys: RowKey[] = [
      {id: 1, str: 'one'},
      {id: 3, str: 'three'},
      {id: 3, str: 'four'}, // Should not match
      {id: 4, str: 'three'}, // Should not match
      {id: 5, str: 'five'},
    ];

    const results = await lookupRowsWithKeys(
      db,
      'public',
      'foo',
      {id: {typeOid: builtins.INT4}, str: {typeOid: builtins.TEXT}},
      rowKeys,
    );
    expect(results).toEqual([
      {id: 1, str: 'one', val: 'foo'},
      {id: 3, str: 'three', val: 'bonk'},
      {id: 5, str: 'five', val: 'boom'},
    ]);
  });

  test('lookupRowsWithJsonKeys', async () => {
    await db.unsafe(`
    CREATE TABLE foo (
      id int,
      key jsonb,
      val text,
      PRIMARY KEY(id, key)
    );
    INSERT INTO foo (id, key, val) VALUES (1, '{"a":1}', 'foo');
    INSERT INTO foo (id, key, val) VALUES (2, '{"a":{"b":2}}', 'bar');
    INSERT INTO foo (id, key, val) VALUES (3, '{}', 'bonk');
    INSERT INTO foo (id, key, val) VALUES (4, '{"c":3}', 'boo');
    INSERT INTO foo (id, key, val) VALUES (5, '{"d":{"e":"f"}}', 'boom');
    `);

    const rowKeys: RowKey[] = [
      {id: 1, key: {a: 1}},
      {id: 3, key: {}},
      {id: 3, key: {b: 2}}, // Should not match
      {id: 4, key: {c: 4}}, // Should not match
      {id: 5, key: {d: {e: 'f'}}},
    ];

    const results = await lookupRowsWithKeys(
      db,
      'public',
      'foo',
      {id: {typeOid: builtins.INT4}, key: {typeOid: builtins.JSONB}},
      rowKeys,
    );
    expect(results).toEqual([
      {id: 1, key: {a: 1}, val: 'foo'},
      {id: 3, key: {}, val: 'bonk'},
      {id: 5, key: {d: {e: 'f'}}, val: 'boom'},
    ]);
  });

  test('multiInsert', async () => {
    await db.unsafe(`
      CREATE TABLE boo (
        foo int,
        bar int,
        baz text,
        "camelCase" int
      );
      `);
    const columns = ['bar', 'baz', 'camelCase', 'foo'] as const;
    const rows1: MyRow[] = [
      {foo: 1, bar: 2, baz: 'three', camelCase: 4},
      {foo: 10, bar: 20, baz: 'thirty', camelCase: 40},
      {foo: 15, bar: 25, baz: 'thirty-five', camelCase: 45},
    ];

    const stmt = multiInsertStatement<MyRow>('public', 'boo', columns, 3);
    await db.unsafe(stmt, multiInsertParams<MyRow>(columns, rows1));

    await expectTables(db, {boo: rows1});

    const rows2: MyRow[] = [
      {foo: 2, bar: 3, baz: 'four', camelCase: 5},
      {foo: 20, bar: 30, baz: 'forty', camelCase: 50},
      {foo: 25, bar: 35, baz: 'forty-five', camelCase: 55},
    ];
    await db.unsafe(stmt, multiInsertParams<MyRow>(columns, rows2));

    await expectTables(db, {boo: [...rows1, ...rows2]});
  });
});
