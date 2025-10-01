import {describe, expect, test} from 'vitest';
import {PgDialect} from 'drizzle-orm/pg-core/dialect';
import type {Row} from '../../../zql/src/mutate/custom.ts';

import {fromDollarParams, toIterableRows} from './drizzle.ts';

const pgDialect = new PgDialect();

describe('fromDollarParams', () => {
  test('binds positional parameters', () => {
    const result = fromDollarParams('select * from users where id = $1', [42]);
    const {sql: sqlParts, params} = pgDialect.sqlToQuery(result);

    expect(sqlParts.trim()).toBe('select * from users where id = $1');
    expect(params).toStrictEqual([42]);
  });

  test('allows literal segments between params', () => {
    const result = fromDollarParams('sum($1::int, $2::int)', [1, 2]);
    const {sql: sqlParts, params} = pgDialect.sqlToQuery(result);

    expect(sqlParts).toBe('sum($1::int, $2::int)');
    expect(params).toStrictEqual([1, 2]);
  });

  test('throws when a parameter is missing', () => {
    expect(() => fromDollarParams('select $1, $2', [1])).toThrow(
      'Missing param for $2',
    );
  });
});

describe('toIterableRows', () => {
  const sampleRows: Row[] = [{id: 1}, {id: 2}];

  test('passes through arrays', () => {
    const iterable = toIterableRows(sampleRows);
    expect([...iterable]).toStrictEqual(sampleRows);
  });

  test('passes through existing iterables', () => {
    const set = new Set<Row>(sampleRows);
    const iterable = toIterableRows(set);
    expect([...iterable]).toStrictEqual([...set]);
  });

  test('extracts rows property from result object', () => {
    const result = {rows: sampleRows};
    const iterable = toIterableRows(result);
    expect([...iterable]).toStrictEqual(sampleRows);
  });

  test('returns empty array for null or undefined results', () => {
    expect([...toIterableRows(null)]).toStrictEqual([]);
    expect([...toIterableRows(undefined)]).toStrictEqual([]);
  });

  test('throws for non-iterable fn without rows', () => {
    expect(() => toIterableRows(() => {})).toThrow(
      'Drizzle query result is not iterable',
    );
  });
});
