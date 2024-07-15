import pg from 'pg';
import type postgres from 'postgres';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../test/db.js';
import {typeNameByOID} from './pg.js';

describe('types/pg-types', () => {
  test('typeNameByIOD', () => {
    const {
      types: {builtins},
    } = pg;

    expect(typeNameByOID[builtins.BYTEA]).toBe('bytea');
    expect(typeNameByOID[builtins.INT4]).toBe('int4');
    expect(typeNameByOID[builtins.TEXT]).toBe('text');
    expect(typeNameByOID[builtins.VARCHAR]).toBe('varchar');
    expect(typeNameByOID[1007]).toBe('int4[]');

    expect(() => (typeNameByOID[1007] = 'should not work')).toThrowError();
    expect(typeNameByOID[1007]).toBe('int4[]');
  });
});

describe('types/pg', () => {
  let db: postgres.Sql<{bigint: bigint}>;

  beforeEach(async () => {
    db = await testDBs.create('pg_types');
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  test('bigints', async () => {
    await db`
    CREATE TABLE foo(
      big int8,
      bigs int8[]
    )`;

    await db`INSERT INTO foo ${db({big: 9007199254740993n})}`;
    expect((await db`SELECT * FROM foo`)[0]).toEqual({
      big: 9007199254740993n,
      bigs: null,
    });

    await db`INSERT INTO foo ${db({bigs: ['9007199254740993']})}`;
    expect((await db`SELECT * FROM foo`)[1]).toEqual({
      big: null,
      bigs: [9007199254740993n],
    });

    // Fails with:
    //   PostgresError: column "bigs" is of type bigint[] but expression is of type bigint
    // Waiting for resolution in:
    //   https://github.com/porsager/postgres/issues/837
    // await db`INSERT INTO foo ${db({bigs: [9007199254740994n]})}`;
    // expect((await db`SELECT * FROM foo`)[2]).toEqual({
    //   big: null,
    //   bigs: [9007199254740994n],
    // });
  });
});
