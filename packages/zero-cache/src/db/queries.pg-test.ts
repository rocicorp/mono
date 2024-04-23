import pg from 'pg';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../test/db.js';
import type {PostgresDB} from '../types/pg.js';
import type {RowKey} from '../types/row-key.js';
import {lookupRowsWithKeys} from './queries.js';

const {
  types: {builtins},
} = pg;

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
});
