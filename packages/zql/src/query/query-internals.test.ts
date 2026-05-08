import {expect, test} from 'vitest';
import {asQueryInternals} from './query-internals.ts';
import type {Query} from './query.ts';

test('asQueryInternals throws with duplicate-Zero hint when tag is missing', () => {
  const fake = {} as Query<string, never, never>;
  expect(() => asQueryInternals(fake)).toThrowError(
    'two copies of Zero in your runtime',
  );
});
