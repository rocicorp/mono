import {expect, test} from 'vitest';
import * as v from '../../shared/src/valita.ts';
import {
  transformResponseMessageSchema,
  type TransformResponseMessage,
} from './custom-queries.ts';

test('transform response schema accepts a third tuple object like {userID}', () => {
  const valid: TransformResponseMessage = ['transformed', []];

  expect(() => v.parse(valid, transformResponseMessageSchema)).not.toThrow();

  expect(() =>
    v.parse(
      ['transformed', [], {userID: 'user-123'}],
      transformResponseMessageSchema,
    ),
  ).not.toThrow();
});
