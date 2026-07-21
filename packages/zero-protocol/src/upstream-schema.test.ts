import {describe, expect, test} from 'vitest';
import * as v from '../../shared/src/valita.ts';
import {upstreamSchema} from './up.ts';

/**
 * Wire-grammar regression suite for `upstreamSchema`.
 *
 * The `protocol-version.test.ts` hash check is a pessimistic fingerprint of
 * the schema's internal class representation: it can fire on additive
 * refactors (e.g. wrapping the union in a chained validator) that do not
 * affect what an old client is allowed to send. These tests are the actual
 * wire-compatibility contract: each upstream message type has a minimal
 * representative value that should round-trip through
 * `upstreamSchema.parse(...)` unchanged, and a handful of obviously-invalid
 * shapes that should be rejected. A passing run here demonstrates that a
 * given schema refactor preserves the wire grammar regardless of what the
 * hash says.
 */

// Each minimal example is the shortest legal payload for that message type
// against the schemas in `up.ts`. If you add a new message type to
// `upstreamSchema`, add a matching example here.
const validExamples: ReadonlyArray<readonly [tag: string, value: unknown]> = [
  [
    'initConnection',
    ['initConnection', {desiredQueriesPatch: []}],
  ],
  ['ping', ['ping', {}]],
  [
    'deleteClients',
    ['deleteClients', {clientIDs: ['c-1']}],
  ],
  [
    'changeDesiredQueries',
    ['changeDesiredQueries', {desiredQueriesPatch: []}],
  ],
  [
    'pull',
    ['pull', {clientGroupID: 'g-1', cookie: null, requestID: 'r-1'}],
  ],
  ['updateAuth', ['updateAuth', {auth: 'tok'}]],
  [
    'push',
    [
      'push',
      {
        clientGroupID: 'g-1',
        mutations: [],
        pushVersion: 1,
        timestamp: 0,
        requestID: 'r-1',
      },
    ],
  ],
  ['closeConnection', ['closeConnection', []]],
  ['inspect.version', ['inspect', {id: 'i-1', op: 'version'}]],
  [
    'inspect.queries',
    ['inspect', {id: 'i-2', op: 'queries'}],
  ],
  [
    'inspect.metrics',
    ['inspect', {id: 'i-3', op: 'metrics'}],
  ],
  [
    'inspect.authenticate',
    ['inspect', {id: 'i-4', op: 'authenticate', value: 'pw'}],
  ],
  [
    'ackMutationResponses',
    ['ackMutationResponses', {id: 1, clientID: 'c-1'}],
  ],
];

const invalidExamples: ReadonlyArray<readonly [name: string, value: unknown]> =
  [
    ['plain object', {}],
    ['empty array', []],
    ['single-element array', ['initConnection']],
    ['unknown message tag', ['unknownMessage', {}]],
    ['initConnection missing body', ['initConnection']],
    [
      'initConnection with wrong body type',
      ['initConnection', 'not-an-object'],
    ],
    [
      'push missing required field (clientGroupID)',
      [
        'push',
        {
          mutations: [],
          pushVersion: 1,
          timestamp: 0,
          requestID: 'r-1',
        },
      ],
    ],
    ['inspect with unknown op', ['inspect', {id: 'i', op: 'nope'}]],
    ['updateAuth with non-string auth', ['updateAuth', {auth: 42}]],
  ];

describe('upstream wire grammar', () => {
  for (const [name, value] of validExamples) {
    test(`accepts ${name}`, () => {
      const parsed = v.parse(value, upstreamSchema);
      expect(parsed).toEqual(value);
    });
  }

  for (const [name, value] of invalidExamples) {
    test(`rejects ${name}`, () => {
      expect(() => v.parse(value, upstreamSchema)).toThrow();
    });
  }
});

describe('upstream depth guard', () => {
  function buildDeepAst(depth: number): unknown {
    let cond: unknown = {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'id'},
      right: {type: 'literal', value: 'x'},
    };
    for (let i = 0; i < depth; i++) {
      cond = {type: 'and', conditions: [cond]};
    }
    return {
      table: 't',
      where: cond,
    };
  }

  test('accepts initConnection with shallow AST', () => {
    const msg = [
      'initConnection',
      {
        desiredQueriesPatch: [
          {
            op: 'put',
            hash: 'h',
            ast: buildDeepAst(5),
            ttl: 60000,
          },
        ],
      },
    ];
    expect(() => v.parse(msg, upstreamSchema)).not.toThrow();
  });

  test('rejects initConnection with deep AST', () => {
    const msg = [
      'initConnection',
      {
        desiredQueriesPatch: [
          {
            op: 'put',
            hash: 'h',
            ast: buildDeepAst(1000),
            ttl: 60000,
          },
        ],
      },
    ];
    expect(() => v.parse(msg, upstreamSchema)).toThrow(/depth/i);
  });

  test('rejects changeDesiredQueries with deep AST', () => {
    const msg = [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {
            op: 'put',
            hash: 'h',
            ast: buildDeepAst(1000),
            ttl: 60000,
          },
        ],
      },
    ];
    expect(() => v.parse(msg, upstreamSchema)).toThrow(/depth/i);
  });
});
