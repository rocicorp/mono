import {expect, test} from 'vitest';
import {transformResponseMessageSchema} from './custom-queries';
import {queryResponseSchema} from './query-server';

test('strips future fields from canonical query responses in strip mode', () => {
  expect(
    queryResponseSchema.parse(
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
      {mode: 'passthrough'},
    ),
  ).toEqual({
    futureQueryPriority: 'future-field',
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

  expect(queryResponseSchema.parse(response, {mode: 'passthrough'})).toEqual(
    transformResponseMessageSchema.parse(response, {mode: 'passthrough'}),
  );
});
