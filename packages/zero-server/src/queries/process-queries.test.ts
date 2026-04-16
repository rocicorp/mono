import {assert, describe, expect, test, vi} from 'vitest';

import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {ApplicationError} from '../../../zero-protocol/src/application-error.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import * as nameMapperModule from '../../../zero-schema/src/name-mapper.ts';
import {QueryParseError} from '../../../zql/src/query/error.ts';
import {queryInternalsTag} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {schema} from '../test/schema.ts';
import {
  handleGetQueriesRequest,
  handleQueryRequest,
} from './process-queries.ts';

const baseQuery = {
  appID: 'test-app',
  schema: 'test-schema',
};

function makeQuery(ast: AST): AnyQuery {
  const query = {
    [queryInternalsTag]: true,
    ast,
    withContext(_ctx: unknown) {
      return query;
    },
  } as unknown as AnyQuery;
  return query;
}

function makeQuerySuccessResponse(
  queries: ReadonlyJSONValue,
  userID: string | null = null,
) {
  return {
    kind: 'QueryResponse',
    userID,
    queries,
  } as const;
}

describe('handleGetQueriesRequest', () => {
  test('returns transformed queries with server names when given JSON body', async () => {
    const ast: AST = {
      table: 'names',
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'b'},
        right: {type: 'literal', value: 'foo'},
      },
    };

    // oxlint-disable-next-line require-await
    const cb = vi.fn(async () => ({query: makeQuery(ast)}));

    const result = await handleGetQueriesRequest(cb, schema, [
      'transform',
      [
        {
          id: 'q1',
          name: 'namesByFoo',
          args: [{foo: 'bar'}],
        },
      ],
    ]);

    expect(cb).toHaveBeenCalledWith('namesByFoo', [{foo: 'bar'}]);
    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          id: 'q1',
          name: 'namesByFoo',
          ast: expect.objectContaining({
            table: 'divergent_names',
            where: expect.objectContaining({
              type: 'simple',
              left: {type: 'column', name: 'divergent_b'},
            }),
          }),
        },
      ]),
    );
  });

  test('reads request bodies from Request instances', async () => {
    const ast: AST = {
      table: 'basic',
      limit: 1,
    };

    // oxlint-disable-next-line require-await
    const cb = vi.fn(async () => ({query: makeQuery(ast)}));

    const body = JSON.stringify([
      'transform',
      [
        {
          id: 'q2',
          name: 'basicLimited',
          args: [],
        },
      ],
    ]);

    const request = new Request('https://example.com/queries', {
      method: 'POST',
      body,
    });

    const result = await handleGetQueriesRequest(cb, schema, request);

    expect(cb).toHaveBeenCalledWith('basicLimited', []);
    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          id: 'q2',
          name: 'basicLimited',
          ast: expect.objectContaining({table: 'basic'}),
        },
      ]),
    );
  });

  test('returns canonical query success when userID and query params are provided', async () => {
    const ast: AST = {
      table: 'basic',
      limit: 1,
    };

    const cb = vi.fn(() => makeQuery(ast));

    const result = await handleQueryRequest(cb, schema, 'user-123', baseQuery, [
      'transform',
      [
        {
          id: 'q2',
          name: 'basicLimited',
          args: [],
        },
      ],
    ]);

    expect(result).toEqual(
      makeQuerySuccessResponse(
        [
          {
            id: 'q2',
            name: 'basicLimited',
            ast: expect.objectContaining({table: 'basic'}),
          },
        ],
        'user-123',
      ),
    );
  });

  test('returns canonical query success with null userID for logged-out requests', async () => {
    const ast: AST = {
      table: 'basic',
    };

    const cb = vi.fn(() => makeQuery(ast));

    const result = await handleQueryRequest(cb, schema, undefined, baseQuery, [
      'transform',
      [
        {
          id: 'q1',
          name: 'basicQuery',
          args: [],
        },
      ],
    ]);

    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          id: 'q1',
          name: 'basicQuery',
          ast: expect.objectContaining({table: 'basic'}),
        },
      ]),
    );
  });

  test('returns transformFailed parse error when validation fails', async () => {
    const result = await handleGetQueriesRequest(
      () => {
        throw new Error('should not be called');
      },
      schema,
      ['invalid', []],
    );

    expect(result).toEqual({
      reason: ErrorReason.Parse,
      kind: expect.stringMatching('TransformFailed'),
      origin: expect.any(String),
      message: expect.stringContaining('Failed to parse getQueries request'),
      queryIDs: [],
      details: expect.objectContaining({name: 'TypeError'}),
    });
  });

  test('returns transformFailed parse error when request body parsing fails', async () => {
    // Create a Request that will fail to parse as JSON
    const request = new Request('https://example.com/queries', {
      method: 'POST',
      body: 'not valid json',
    });

    const result = await handleGetQueriesRequest(
      () => {
        throw new Error('should not be called');
      },
      schema,
      request,
    );

    expect(result).toEqual({
      reason: ErrorReason.Parse,
      kind: expect.stringMatching('TransformFailed'),
      origin: expect.any(String),
      message: expect.stringContaining('Failed to parse getQueries request'),
      details: expect.objectContaining({name: 'SyntaxError'}),
      queryIDs: [],
    });
  });

  test('marks failed queries with app error and continues processing remaining queries', async () => {
    const ast: AST = {
      table: 'basic',
    };

    // oxlint-disable-next-line require-await
    const cb = vi.fn(async name => {
      if (name === 'first') {
        throw new Error('callback failed');
      }
      return {query: makeQuery(ast)};
    });

    const result = await handleGetQueriesRequest(cb, schema, [
      'transform',
      [
        {id: 'q1', name: 'first', args: []},
        {id: 'q2', name: 'second', args: []},
      ],
    ]);

    expect(cb).toHaveBeenCalledTimes(2);
    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          error: 'app',
          id: 'q1',
          name: 'first',
          message: 'callback failed',
        },
        {
          id: 'q2',
          name: 'second',
          ast: expect.objectContaining({table: 'basic'}),
        },
      ]),
    );
  });

  test('wraps thrown errors from callback with details when possible', async () => {
    const error = new TypeError('custom type error');
    const cb = vi.fn(() => {
      throw error;
    });

    const result = await handleGetQueriesRequest(cb, schema, [
      'transform',
      [{id: 'q1', name: 'test', args: []}],
    ]);

    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          error: 'app',
          id: 'q1',
          name: 'test',
          message: 'custom type error',
          details: expect.objectContaining({name: 'TypeError'}),
        },
      ]),
    );
  });

  test('retains custom details from ApplicationError', async () => {
    const customDetails = {code: 'CUSTOM_ERROR', context: {foo: 'bar'}};
    const error = new ApplicationError('Application specific error', {
      details: customDetails,
    });

    const cb = vi.fn(() => {
      throw error;
    });

    const result = await handleGetQueriesRequest(cb, schema, [
      'transform',
      [{id: 'q1', name: 'test', args: []}],
    ]);

    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          error: 'app',
          id: 'q1',
          name: 'test',
          message: 'Application specific error',
          details: customDetails,
        },
      ]),
    );
  });

  test('marks QueryParseError as parse error instead of app error', async () => {
    const parseError = new QueryParseError({
      cause: new TypeError('Invalid argument type'),
    });

    const cb = vi.fn(() => {
      throw parseError;
    });

    const result = await handleGetQueriesRequest(cb, schema, [
      'transform',
      [{id: 'q1', name: 'testQuery', args: [{foo: 'bar'}]}],
    ]);

    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          error: 'parse',
          id: 'q1',
          name: 'testQuery',
          message: 'Failed to parse arguments for query: Invalid argument type',
          details: expect.objectContaining({name: 'QueryParseError'}),
        },
      ]),
    );
  });

  test('marks QueryParseError as parse error and continues processing remaining queries', async () => {
    const ast: AST = {
      table: 'basic',
    };

    // oxlint-disable-next-line require-await
    const cb = vi.fn(async name => {
      if (name === 'parseErrorQuery') {
        throw new QueryParseError({
          cause: new Error('Invalid args'),
        });
      }
      return {query: makeQuery(ast)};
    });

    const result = await handleGetQueriesRequest(cb, schema, [
      'transform',
      [
        {id: 'q1', name: 'parseErrorQuery', args: []},
        {id: 'q2', name: 'successQuery', args: []},
      ],
    ]);

    expect(cb).toHaveBeenCalledTimes(2);
    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          error: 'parse',
          id: 'q1',
          name: 'parseErrorQuery',
          message: 'Failed to parse arguments for query: Invalid args',
          details: expect.objectContaining({name: 'QueryParseError'}),
        },
        {
          id: 'q2',
          name: 'successQuery',
          ast: expect.objectContaining({table: 'basic'}),
        },
      ]),
    );
  });

  test('returns transformFailed for infrastructure errors during schema processing', async () => {
    const ast: AST = {
      table: 'basic',
    };

    // Mock clientToServer to throw an infrastructure error
    const spy = vi
      .spyOn(nameMapperModule, 'clientToServer')
      .mockImplementation(() => {
        throw new TypeError('Schema processing failed');
      });

    try {
      // oxlint-disable-next-line require-await
      const cb = vi.fn(async () => ({query: makeQuery(ast)}));

      const result = await handleGetQueriesRequest(cb, schema, [
        'transform',
        [{id: 'q1', name: 'test', args: []}],
      ]);

      assert(
        result.kind === 'TransformFailed',
        'Expected transformFailed tuple response',
      );
      expect(result).toEqual({
        reason: ErrorReason.Internal,
        kind: expect.any(String),
        origin: expect.any(String),
        message: 'Schema processing failed',
        queryIDs: ['q1'],
        details: expect.objectContaining({name: 'TypeError'}),
      });
    } finally {
      spy.mockRestore();
    }
  });
});

describe('handleQueryRequest', () => {
  test('returns transformed queries with server names when given JSON body', async () => {
    const ast: AST = {
      table: 'names',
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'b'},
        right: {type: 'literal', value: 'foo'},
      },
    };

    const cb = vi.fn(() => makeQuery(ast));

    const result = await handleQueryRequest(cb, schema, undefined, baseQuery, [
      'transform',
      [
        {
          id: 'q1',
          name: 'namesByFoo',
          args: [{foo: 'bar'}],
        },
      ],
    ]);

    expect(cb).toHaveBeenCalledWith('namesByFoo', {foo: 'bar'});
    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          id: 'q1',
          name: 'namesByFoo',
          ast: expect.objectContaining({
            table: 'divergent_names',
            where: expect.objectContaining({
              type: 'simple',
              left: {type: 'column', name: 'divergent_b'},
            }),
          }),
        },
      ]),
    );
  });

  test('reads request bodies from Request instances', async () => {
    const ast: AST = {
      table: 'basic',
      limit: 1,
    };

    const cb = vi.fn(() => makeQuery(ast));

    const body = JSON.stringify([
      'transform',
      [
        {
          id: 'q2',
          name: 'basicLimited',
          args: [],
        },
      ],
    ]);

    const request = new Request('https://example.com/queries', {
      method: 'POST',
      body,
    });

    const result = await handleQueryRequest(cb, schema, undefined, request);

    expect(cb).toHaveBeenCalledWith('basicLimited', undefined);
    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          id: 'q2',
          name: 'basicLimited',
          ast: expect.objectContaining({table: 'basic'}),
        },
      ]),
    );
  });

  test('returns transformFailed parse error when validation fails', async () => {
    const result = await handleQueryRequest(
      () => {
        throw new Error('should not be called');
      },
      schema,
      undefined,
      baseQuery,
      ['invalid', []],
    );

    expect(result).toEqual({
      kind: expect.any(String),
      message: expect.stringContaining('Failed to parse query request'),
      origin: expect.any(String),
      queryIDs: [],
      reason: ErrorReason.Parse,
      details: expect.objectContaining({name: 'TypeError'}),
    });
  });

  test('returns transformFailed parse error when request body parsing fails', async () => {
    // Create a Request that will fail to parse as JSON
    const request = new Request('https://example.com/queries', {
      method: 'POST',
      body: 'not valid json',
    });

    const result = await handleQueryRequest(
      () => {
        throw new Error('should not be called');
      },
      schema,
      undefined,
      request,
    );

    expect(result).toEqual({
      reason: ErrorReason.Parse,
      kind: expect.any(String),
      origin: expect.any(String),
      message: expect.stringContaining('Failed to parse query request'),
      details: expect.objectContaining({name: 'SyntaxError'}),
      queryIDs: [],
    });
  });

  test('marks failed queries with app error and continues processing remaining queries', async () => {
    const ast: AST = {
      table: 'basic',
    };

    const cb = vi.fn(name => {
      if (name === 'first') {
        throw new Error('callback failed');
      }
      return makeQuery(ast);
    });

    const result = await handleQueryRequest(cb, schema, undefined, baseQuery, [
      'transform',
      [
        {id: 'q1', name: 'first', args: []},
        {id: 'q2', name: 'second', args: []},
      ],
    ]);

    expect(cb).toHaveBeenCalledTimes(2);
    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          error: 'app',
          id: 'q1',
          name: 'first',
          message: 'callback failed',
        },
        {
          id: 'q2',
          name: 'second',
          ast: expect.objectContaining({table: 'basic'}),
        },
      ]),
    );
  });

  test('wraps thrown errors from callback with details when possible', async () => {
    const error = new TypeError('custom type error');
    const cb = vi.fn(() => {
      throw error;
    });

    const result = await handleQueryRequest(cb, schema, undefined, baseQuery, [
      'transform',
      [{id: 'q1', name: 'test', args: []}],
    ]);

    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          error: 'app',
          id: 'q1',
          name: 'test',
          message: 'custom type error',
          details: expect.objectContaining({name: 'TypeError'}),
        },
      ]),
    );
  });

  test('retains custom details from ApplicationError', async () => {
    const customDetails = {code: 'CUSTOM_ERROR', context: {foo: 'bar'}};
    const error = new ApplicationError('Application specific error', {
      details: customDetails,
    });

    const cb = vi.fn(() => {
      throw error;
    });

    const result = await handleQueryRequest(cb, schema, undefined, baseQuery, [
      'transform',
      [{id: 'q1', name: 'test', args: []}],
    ]);

    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          error: 'app',
          id: 'q1',
          name: 'test',
          message: 'Application specific error',
          details: customDetails,
        },
      ]),
    );
  });

  test('marks QueryParseError as parse error instead of app error', async () => {
    const parseError = new QueryParseError({
      cause: new TypeError('Invalid argument type'),
    });

    const cb = vi.fn(() => {
      throw parseError;
    });

    const result = await handleQueryRequest(cb, schema, undefined, baseQuery, [
      'transform',
      [{id: 'q1', name: 'testQuery', args: [{foo: 'bar'}]}],
    ]);

    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          error: 'parse',
          id: 'q1',
          name: 'testQuery',
          message: 'Failed to parse arguments for query: Invalid argument type',
          details: expect.objectContaining({name: 'QueryParseError'}),
        },
      ]),
    );
  });

  test('marks QueryParseError as parse error and continues processing remaining queries', async () => {
    const ast: AST = {
      table: 'basic',
    };

    const cb = vi.fn(name => {
      if (name === 'parseErrorQuery') {
        throw new QueryParseError({
          cause: new Error('Invalid args'),
        });
      }
      return makeQuery(ast);
    });

    const result = await handleQueryRequest(cb, schema, undefined, baseQuery, [
      'transform',
      [
        {id: 'q1', name: 'parseErrorQuery', args: []},
        {id: 'q2', name: 'successQuery', args: []},
      ],
    ]);

    expect(cb).toHaveBeenCalledTimes(2);
    expect(result).toEqual(
      makeQuerySuccessResponse([
        {
          error: 'parse',
          id: 'q1',
          name: 'parseErrorQuery',
          message: 'Failed to parse arguments for query: Invalid args',
          details: expect.objectContaining({name: 'QueryParseError'}),
        },
        {
          id: 'q2',
          name: 'successQuery',
          ast: expect.objectContaining({table: 'basic'}),
        },
      ]),
    );
  });

  test('returns transformFailed for infrastructure errors during schema processing', async () => {
    const ast: AST = {
      table: 'basic',
    };

    // Mock clientToServer to throw an infrastructure error
    using _spy = vi
      .spyOn(nameMapperModule, 'clientToServer')
      .mockImplementation(() => {
        throw new TypeError('Schema processing failed');
      });

    const cb = vi.fn(() => makeQuery(ast));

    const result = await handleQueryRequest(cb, schema, undefined, baseQuery, [
      'transform',
      [{id: 'q1', name: 'test', args: []}],
    ]);

    expect(result).toEqual({
      kind: expect.any(String),
      message: 'Schema processing failed',
      origin: expect.any(String),
      queryIDs: ['q1'],
      reason: ErrorReason.Internal,
      details: expect.objectContaining({name: 'TypeError'}),
    });
  });
});
