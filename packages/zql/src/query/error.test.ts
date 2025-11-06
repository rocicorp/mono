import {assert, expect, test} from 'vitest';
import * as v from '../../../shared/src/valita.ts';
import {QueryParseError} from './error.ts';

test('QueryParseError preserves cause and message', () => {
  const schema = v.tuple([v.string()]);

  let cause: Error | undefined;
  try {
    v.parse([123], schema);
  } catch (error) {
    cause = error as Error;
  }

  assert(cause, 'expected valita.parse to throw');

  const queryError = new QueryParseError({cause});

  expect(queryError.cause).toBe(cause);
  expect(queryError.message).toMatchInlineSnapshot(
    `"Failed to parse arguments for query: Expected string at 0. Got 123"`,
  );
  expect(queryError.name).toBe('QueryParseError');
});
