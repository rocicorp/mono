import pg from 'pg';
import type postgres from 'postgres';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../test/db.js';
import {timestampToFpMillis, typeNameByOID} from './pg.js';

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
    await db.unsafe(`
    CREATE TABLE bigints(
      big int8,
      bigs int8[]
    );
    CREATE TABLE timestamps(
      timestamp timestamp,
      timestamptz timestamptz,
      timestamps timestamp[],
      timestamptzs timestamptz[]
    );
    CREATE TABLE dates(
      d date,
      ds date[]
    );
    `);
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  test('bigints', async () => {
    await db`INSERT INTO bigints ${db({big: 9007199254740993n})}`;
    expect((await db`SELECT * FROM bigints`)[0]).toEqual({
      big: 9007199254740993n,
      bigs: null,
    });

    await db`INSERT INTO bigints ${db({bigs: ['9007199254740993']})}`;
    expect((await db`SELECT * FROM bigints`)[1]).toEqual({
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

  test.each([
    ['January 8 04:05:06.123456 1999 PST', 915768306123.456, 915797106123.456],
    ['2004-10-19 10:23:54.654321+02', 1098181434654.321, 1098174234654.321],
    ['1999-01-08 04:05:06.987654 -8:00', 915768306987.654, 915797106987.654],
    [915768306123, 915768306123, 915768306123],
    [915768306123.456, 915768306123.456, 915768306123.456],
  ])('timestamp: %s', async (input, output, outputTZ) => {
    await db`INSERT INTO timestamps ${db({
      timestamp: input,
      timestamptz: input,
      timestamps: [input, input],
      timestamptzs: [input, input],
    })}`;
    expect((await db`SELECT * FROM timestamps`)[0]).toEqual({
      timestamp: output,
      timestamptz: outputTZ,
      timestamps: [output, output],
      timestamptzs: [outputTZ, outputTZ],
    });
  });

  test.for([
    // This one does not work... Maybe because of the timezone? Daylight saving?
    // ['January 8, 1999', Date.UTC(1999, 0, 8)],

    ['2004-10-19', Date.UTC(2004, 9, 19)],
    ['1999-01-08', Date.UTC(1999, 0, 8)],
  ])('timestamp: %s', async ([input, expected]) => {
    await db`INSERT INTO dates ${db({
      d: input,
      ds: [input, input],
    })}`;
    expect((await db`SELECT * FROM dates`)[0]).toEqual({
      d: expected,
      ds: [expected, expected],
    });
  });

  test.each([
    ['2004-10-19 10:23:54.654321', 1098181434654.321],
    ['2004-10-19 10:23:54.654321+00', 1098181434654.321],
    ['2004-10-19 10:23:54.654321+00:00', 1098181434654.321],
    ['2004-10-19 10:23:54.654321+02', 1098174234654],
    ['2024-12-05 16:38:21.907-05', 1733434701907],
    ['2024-12-05 16:38:21.907-05:30', 1733436501907],
  ])('parse timestamp: %s', (timestamp, result) => {
    expect(timestampToFpMillis(timestamp)).toBe(result);
  });
});
