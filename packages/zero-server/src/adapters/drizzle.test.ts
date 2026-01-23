import {describe, expect, test} from 'vitest';
import type {Row} from '../../../zql/src/mutate/custom.ts';

import {toIterableRows} from './drizzle.ts';

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
