import {assert, describe, expect, test, vi} from 'vitest';

import {handleGetQueriesRequest} from './process-queries.ts';
import type {AnyQuery} from '../../../zql/src/query/query-impl.ts';
import {schema} from '../test/schema.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {ApplicationError} from '../error.ts';

function makeQuery(ast: AST): AnyQuery {
  return {ast} as unknown as AnyQuery;
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
    expect(result[0]).toBe('transformed');
    assert(result[0] === 'transformed');
    const [response] = result[1];
    assert(!('error' in response));
    expect(response).toMatchObject({id: 'q1', name: 'namesByFoo'});
    expect(response.ast.table).toBe('divergent_names');

    const where = response.ast.where;
    assert(where && where.type === 'simple', 'expected simple where clause');
    expect(where.left).toEqual({type: 'column', name: 'divergent_b'});
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

    const request = new Request('https://example.com/get-queries', {
      method: 'POST',
      body,
    });

    const result = await handleGetQueriesRequest(cb, schema, request);

    expect(cb).toHaveBeenCalledWith('basicLimited', []);
    expect(result).toEqual([
      'transformed',
      [
        {
          id: 'q2',
          name: 'basicLimited',
          ast: expect.objectContaining({table: 'basic'}),
        },
      ],
    ]);
  });

  test('returns transformFailed parse error when validation fails', async () => {
    const result = await handleGetQueriesRequest(
      () => {
        throw new Error('should not be called');
      },
      schema,
      ['invalid', []],
    );

    expect(result[0]).toBe('transformFailed');
    expect(result[1]).toMatchObject({
      type: 'parse',
      kind: expect.any(String),
      origin: expect.any(String),
      message: expect.stringContaining('Failed to parse get queries request'),
      queryIDs: [],
      details: expect.objectContaining({name: 'TypeError'}),
    });
  });

  test('returns transformFailed parse error when request body parsing fails', async () => {
    // Create a Request that will fail to parse as JSON
    const request = new Request('https://example.com/get-queries', {
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

    expect(result[0]).toBe('transformFailed');
    expect(result[1]).toMatchObject({
      type: 'parse',
      kind: expect.any(String),
      origin: expect.any(String),
      message: expect.stringContaining('Failed to parse get queries request'),
      queryIDs: [],
    });
  });

  test('returns transformFailed internal error when callback throws and preserves query IDs', async () => {
    const error = new Error('callback failed');
    const cb = vi.fn(() => {
      throw error;
    });

    const result = await handleGetQueriesRequest(cb, schema, [
      'transform',
      [
        {id: 'q1', name: 'first', args: []},
        {id: 'q2', name: 'second', args: []},
      ],
    ]);

    expect(cb).toHaveBeenCalled();
    expect(result[0]).toBe('transformFailed');
    expect(result[1]).toMatchObject({
      type: 'internal',
      message: 'callback failed',
      queryIDs: ['q1', 'q2'],
    });
  });

  test('returns transformFailed internal error with details when callback throws custom error', async () => {
    const error = new TypeError('custom type error');
    const cb = vi.fn(() => {
      throw error;
    });

    const result = await handleGetQueriesRequest(cb, schema, [
      'transform',
      [{id: 'q1', name: 'test', args: []}],
    ]);

    expect(result[0]).toBe('transformFailed');
    expect(result[1]).toMatchObject({
      type: 'internal',
      message: 'custom type error',
      queryIDs: ['q1'],
      details: expect.objectContaining({name: 'TypeError'}),
    });
  });

  test('returns transformFailed with custom details from ApplicationError', async () => {
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

    expect(result[0]).toBe('transformFailed');
    expect(result[1]).toMatchObject({
      type: 'internal',
      message: 'Application specific error',
      queryIDs: ['q1'],
      details: customDetails,
    });
  });
});
