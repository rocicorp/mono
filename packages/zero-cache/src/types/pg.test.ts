import {PreciseDate} from '@google-cloud/precise-date';
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
    )`);
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
    [
      'January 8 04:05:06.123456 1999 PST',
      915768306123456000n,
      915797106123456000n,
    ],
    [
      '2004-10-19 10:23:54.654321+02',
      1098181434654321000n,
      1098174234654321000n,
    ],
    [
      '1999-01-08 04:05:06.987654 -8:00',
      915768306987654000n,
      915797106987654000n,
    ],
  ])('timestamp: %s', async (input, output, outputTZ) => {
    await db`INSERT INTO timestamps ${db({
      timestamp: input,
      timestamptz: input,
      timestamps: [input, input],
      timestamptzs: [input, input],
    })}`;
    const timestamp = new PreciseDate(output);
    const timestamptz = new PreciseDate(outputTZ);
    expect((await db`SELECT * FROM timestamps`)[0]).toEqual({
      timestamp,
      timestamptz,
      timestamps: [timestamp, timestamp],
      timestamptzs: [timestamptz, timestamptz],
    });
  });
});
