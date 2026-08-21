import {expect, test} from 'vitest';
import * as v from '../../shared/src/valita.ts';
import {upPutOpSchema} from './queries-patch.ts';
import {upstreamSchema} from './up.ts';

function uninspectableAST() {
  return new Proxy(
    {},
    {
      get() {
        throw new Error('AST was inspected');
      },
      ownKeys() {
        throw new Error('AST was inspected');
      },
    },
  );
}

test('accepts a custom query put', () => {
  const put = {
    op: 'put',
    hash: 'query-hash',
    name: 'issuesByAssignee',
    args: ['user-1'],
    ttl: 60_000,
  };

  expect(v.parse(put, upPutOpSchema)).toEqual(put);
});

test('accepts a named custom query for inspector analysis', () => {
  const message = [
    'inspect',
    {
      id: 'inspect-1',
      op: 'analyze-query',
      name: 'issuesByAssignee',
      args: ['user-1'],
    },
  ];

  expect(v.parse(message, upstreamSchema)).toEqual(message);
});

test('rejects a legacy query put without inspecting its AST', () => {
  expect(() =>
    v.parse(
      {
        op: 'put',
        hash: 'query-hash',
        ast: uninspectableAST(),
      },
      upPutOpSchema,
    ),
  ).toThrow(/Legacy queries are not supported/);
});

test.each(['initConnection', 'changeDesiredQueries'] as const)(
  'rejects a legacy query in %s without inspecting its AST',
  messageType => {
    expect(() =>
      v.parse(
        [
          messageType,
          {
            desiredQueriesPatch: [
              {
                op: 'put',
                hash: 'query-hash',
                ast: uninspectableAST(),
              },
            ],
          },
        ],
        upstreamSchema,
      ),
    ).toThrow(/Legacy queries are not supported/);
  },
);

test.each(['ast', 'value'] as const)(
  'rejects an inspector analyze-query %s without inspecting it',
  field => {
    expect(() =>
      v.parse(
        [
          'inspect',
          {
            id: 'inspect-1',
            op: 'analyze-query',
            [field]: uninspectableAST(),
          },
        ],
        upstreamSchema,
      ),
    ).toThrow(/Legacy queries are not supported/);
  },
);
