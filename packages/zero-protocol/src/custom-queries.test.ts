import {expect, test} from 'vitest';

import {
  apiQueryResponseSchema,
  transformResponseMessageSchema,
} from './custom-queries.ts';

test('strips future fields from canonical query responses in strip mode', () => {
  expect(
    apiQueryResponseSchema.parse(
      {
        kind: 'QueryResponse',
        userID: 'user-123',
        queries: [
          {
            id: 'q1',
            name: 'issues',
            ast: {
              table: 'issue',
            },
          },
        ],
        futureQueryPriority: 'future-field',
      },
      {mode: 'strip'},
    ),
  ).toEqual({
    kind: 'QueryResponse',
    userID: 'user-123',
    queries: [
      {
        id: 'q1',
        name: 'issues',
        ast: {
          table: 'issue',
        },
      },
    ],
  });
});

test('parses legacy transform responses through the /query API schema', () => {
  const response = [
    'transformed',
    [
      {
        id: 'q1',
        name: 'issues',
        ast: {
          table: 'issue',
        },
      },
    ],
  ] as const;

  expect(apiQueryResponseSchema.parse(response, {mode: 'strip'})).toEqual(
    transformResponseMessageSchema.parse(response, {mode: 'strip'}),
  );
});
