import {describe, expect, test, vi} from 'vitest';

import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {ApplicationError} from '../../../zero-protocol/src/application-error.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import * as nameMapperModule from '../../../zero-schema/src/name-mapper.ts';
import {QueryParseError} from '../../../zql/src/query/error.ts';
import {queryInternalsTag} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {schema} from '../test/schema.ts';
import {handleQueryRequest} from './process-queries.ts';

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

function makeCanonicalQuerySuccessResponse(
  queries: ReadonlyJSONValue,
  userID: string | null,
) {
  return {
    kind: 'QueryResponse',
    userID,
    queries,
  } as const;
}

function makeLegacyQuerySuccessResponse(queries: ReadonlyJSONValue) {
  return {
    kind: 'QueryResponse',
    queries,
  } as const;
}

describe('handleQueryRequest', () => {
  test('returns transformed queries with server names for body inputs', async () => {
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

    const result = await handleQueryRequest({
      handler: cb,
      schema,
      body: [
        'transform',
        [
          {
            id: 'q1',
            name: 'namesByFoo',
            args: [{foo: 'bar'}],
          },
        ],
      ],
      userID: null,
    });

    expect(cb).toHaveBeenCalledWith('namesByFoo', [{foo: 'bar'}]);
    expect(result).toEqual(
      makeCanonicalQuerySuccessResponse(
        [
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
        ],
        null,
      ),
    );
  });

  test('reads request bodies from Request instances and echoes userID', async () => {
    const ast: AST = {
      table: 'basic',
      limit: 1,
    };

    const cb = vi.fn(() => makeQuery(ast));

    const request = new Request('https://example.com/queries', {
      method: 'POST',
      body: JSON.stringify([
        'transform',
        [
          {
            id: 'q2',
            name: 'basicLimited',
            args: [],
          },
        ],
      ]),
    });

    const result = await handleQueryRequest({
      handler: cb,
      schema,
      request,
      userID: 'user-123',
      logLevel: 'debug',
    });

    expect(cb).toHaveBeenCalledWith('basicLimited', []);
    expect(result).toEqual(
      makeCanonicalQuerySuccessResponse(
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

  test('normalizes undefined userID to null for object-form requests', async () => {
    const ast: AST = {
      table: 'basic',
    };

    const cb = vi.fn(() => makeQuery(ast));

    const result = await handleQueryRequest({
      handler: cb,
      schema,
      body: [
        'transform',
        [
          {
            id: 'q1',
            name: 'basicQuery',
            args: [],
          },
        ],
      ],
      userID: undefined,
    });

    expect(result).toEqual(
      makeCanonicalQuerySuccessResponse(
        [
          {
            id: 'q1',
            name: 'basicQuery',
            ast: expect.objectContaining({table: 'basic'}),
          },
        ],
        null,
      ),
    );
    expect(result).toHaveProperty('userID', null);
  });

  test('returns transformFailed parse error when validation fails', async () => {
    const result = await handleQueryRequest({
      handler: () => {
        throw new Error('should not be called');
      },
      schema,
      body: ['invalid', []],
      userID: null,
    });

    expect(result).toEqual({
      reason: ErrorReason.Parse,
      kind: expect.stringMatching('TransformFailed'),
      origin: expect.any(String),
      message: expect.stringContaining('Failed to parse query request'),
      queryIDs: [],
      details: expect.objectContaining({name: 'TypeError'}),
    });
  });

  test('returns transformFailed parse error when request body parsing fails', async () => {
    const request = new Request('https://example.com/queries', {
      method: 'POST',
      body: 'not valid json',
    });

    const result = await handleQueryRequest({
      handler: () => {
        throw new Error('should not be called');
      },
      schema,
      request,
      userID: null,
    });

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

    const result = await handleQueryRequest({
      handler: cb,
      schema,
      body: [
        'transform',
        [
          {id: 'q1', name: 'first', args: []},
          {id: 'q2', name: 'second', args: []},
        ],
      ],
      userID: null,
    });

    expect(cb).toHaveBeenCalledTimes(2);
    expect(result).toEqual(
      makeCanonicalQuerySuccessResponse(
        [
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
        ],
        null,
      ),
    );
  });

  test('wraps thrown errors from callback with details when possible', async () => {
    const error = new TypeError('custom type error');
    const cb = vi.fn(() => {
      throw error;
    });

    const result = await handleQueryRequest({
      handler: cb,
      schema,
      body: ['transform', [{id: 'q1', name: 'test', args: []}]],
      userID: null,
    });

    expect(result).toEqual(
      makeCanonicalQuerySuccessResponse(
        [
          {
            error: 'app',
            id: 'q1',
            name: 'test',
            message: 'custom type error',
            details: expect.objectContaining({name: 'TypeError'}),
          },
        ],
        null,
      ),
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

    const result = await handleQueryRequest({
      handler: cb,
      schema,
      body: ['transform', [{id: 'q1', name: 'test', args: []}]],
      userID: null,
    });

    expect(result).toEqual(
      makeCanonicalQuerySuccessResponse(
        [
          {
            error: 'app',
            id: 'q1',
            name: 'test',
            message: 'Application specific error',
            details: customDetails,
          },
        ],
        null,
      ),
    );
  });

  test('marks QueryParseError as parse error instead of app error', async () => {
    const parseError = new QueryParseError({
      cause: new TypeError('Invalid argument type'),
    });

    const cb = vi.fn(() => {
      throw parseError;
    });

    const result = await handleQueryRequest({
      handler: cb,
      schema,
      body: [
        'transform',
        [{id: 'q1', name: 'testQuery', args: [{foo: 'bar'}]}],
      ],
      userID: null,
    });

    expect(result).toEqual(
      makeCanonicalQuerySuccessResponse(
        [
          {
            error: 'parse',
            id: 'q1',
            name: 'testQuery',
            message:
              'Failed to parse arguments for query: Invalid argument type',
            details: expect.objectContaining({name: 'QueryParseError'}),
          },
        ],
        null,
      ),
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

    const result = await handleQueryRequest({
      handler: cb,
      schema,
      body: [
        'transform',
        [
          {id: 'q1', name: 'parseErrorQuery', args: []},
          {id: 'q2', name: 'successQuery', args: []},
        ],
      ],
      userID: null,
    });

    expect(cb).toHaveBeenCalledTimes(2);
    expect(result).toEqual(
      makeCanonicalQuerySuccessResponse(
        [
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
        ],
        null,
      ),
    );
  });

  test('returns transformFailed for infrastructure errors during schema processing', async () => {
    const ast: AST = {
      table: 'basic',
    };

    using _spy = vi
      .spyOn(nameMapperModule, 'clientToServer')
      .mockImplementation(() => {
        throw new TypeError('Schema processing failed');
      });

    const cb = vi.fn(() => makeQuery(ast));

    const result = await handleQueryRequest({
      handler: cb,
      schema,
      body: ['transform', [{id: 'q1', name: 'test', args: []}]],
      userID: null,
    });

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

describe('handleQueryRequest backwards compatibility', () => {
  test('supports positional body inputs and omits userID', async () => {
    const ast: AST = {
      table: 'basic',
    };

    const result = await handleQueryRequest(() => makeQuery(ast), schema, [
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
      makeLegacyQuerySuccessResponse([
        {
          id: 'q1',
          name: 'basicQuery',
          ast: expect.objectContaining({table: 'basic'}),
        },
      ]),
    );
    expect(result).not.toHaveProperty('userID');
  });

  test('supports positional Request inputs and omits userID', async () => {
    const ast: AST = {
      table: 'basic',
      limit: 1,
    };

    const request = new Request('https://example.com/queries', {
      method: 'POST',
      body: JSON.stringify([
        'transform',
        [
          {
            id: 'q2',
            name: 'basicLimited',
            args: [],
          },
        ],
      ]),
    });

    const result = await handleQueryRequest(
      () => makeQuery(ast),
      schema,
      request,
    );

    expect(result).toEqual(
      makeLegacyQuerySuccessResponse([
        {
          id: 'q2',
          name: 'basicLimited',
          ast: expect.objectContaining({table: 'basic'}),
        },
      ]),
    );
    expect(result).not.toHaveProperty('userID');
  });
});
