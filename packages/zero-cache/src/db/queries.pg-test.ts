import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import type postgres from 'postgres';
import {testDBs} from '../test/db.js';
import type {RowKeyValue} from '../types/row-key.js';
import {lookupRowsWithKeys} from './queries.js';

describe('db/queries', () => {
  let db: postgres.Sql;

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

    const rowKeys: RowKeyValue[] = [
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
      {id: {typeOid: 23}, str: {typeOid: 25}},
      rowKeys,
    );
    expect(results).toEqual([
      {id: 1, str: 'one', val: 'foo'},
      {id: 3, str: 'three', val: 'bonk'},
      {id: 5, str: 'five', val: 'boom'},
    ]);
  });
});
