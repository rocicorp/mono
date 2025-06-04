import {
  describe,
  expect,
  vi,
  beforeEach,
  type MockedFunction,
  test,
} from 'vitest';
import {transformCustomQueries} from './transform-query.ts';
import {fetchFromAPIServer} from '../custom/fetch.ts';
import type {CustomQueryRecord} from '../services/view-syncer/schema/types.ts';
import type {ShardID} from '../types/shards.ts';
import type {TransformResponseMessage} from '../../../zero-protocol/src/custom-queries.ts';

// Mock the fetchFromAPIServer function
vi.mock('../custom/fetch.ts');
const mockFetchFromAPIServer = fetchFromAPIServer as MockedFunction<
  typeof fetchFromAPIServer
>;

describe('transformCustomQueries', () => {
  const mockShard: ShardID = {
    appID: 'test_app',
    shardNum: 1,
  };

  const pullUrl = 'https://api.example.com/pull';
  const headerOptions = {
    apiKey: 'test-api-key',
    token: 'test-token',
  };

  const mockQueries: CustomQueryRecord[] = [
    {
      id: 'query1',
      type: 'custom',
      name: 'getUserById',
      args: [123],
      clientState: {},
    },
    {
      id: 'query2',
      type: 'custom',
      name: 'getPostsByUser',
      args: ['user123', 10],
      clientState: {},
    },
  ];

  beforeEach(() => {
    mockFetchFromAPIServer.mockReset();
  });

  test('should transform queries successfully and return TransformedAndHashed array', async () => {
    const mockSuccessResponse = new Response(
      JSON.stringify([
        'transformed',
        [
          {
            id: 'hash1',
            name: 'getUserById',
            ast: {
              table: 'users',
              where: {
                type: 'simple',
                op: '=',
                left: {type: 'column', name: 'id'},
                right: {type: 'literal', value: 123},
              },
            },
          },
          {
            id: 'hash2',
            name: 'getPostsByUser',
            ast: {
              table: 'posts',
              where: {
                type: 'simple',
                op: '=',
                left: {type: 'column', name: 'userId'},
                right: {type: 'literal', value: 'user123'},
              },
            },
          },
        ],
      ] satisfies TransformResponseMessage),
      {status: 200},
    );

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse);

    const result = await transformCustomQueries(
      pullUrl,
      mockShard,
      headerOptions,
      mockQueries,
    );

    // Verify the API was called correctly
    expect(mockFetchFromAPIServer).toHaveBeenCalledWith(
      pullUrl,
      mockShard,
      headerOptions,
      undefined,
      [
        'transform',
        [
          {id: 'query1', name: 'getUserById', args: [123]},
          {id: 'query2', name: 'getPostsByUser', args: ['user123', 10]},
        ],
      ],
    );

    // Verify the result
    expect(result).toEqual([
      {
        query: {
          table: 'users',
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'id'},
            right: {type: 'literal', value: 123},
          },
        },
        hash: 'hash1',
      },
      {
        query: {
          table: 'posts',
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'userId'},
            right: {type: 'literal', value: 'user123'},
          },
        },
        hash: 'hash2',
      },
    ]);
  });

  test('should handle errored queries in response', async () => {
    const mockMixedResponse = new Response(
      JSON.stringify([
        'transformed',
        [
          {
            id: 'hash1',
            name: 'getUserById',
            ast: {
              table: 'users',
              where: {
                type: 'simple',
                op: '=',
                left: {type: 'column', name: 'id'},
                right: {type: 'literal', value: 123},
              },
            },
          },
          {
            error: 'app',
            id: 'query2',
            name: 'getPostsByUser',
            details: 'Query syntax error',
          },
        ],
      ] satisfies TransformResponseMessage),
      {status: 200},
    );

    mockFetchFromAPIServer.mockResolvedValue(mockMixedResponse);

    const result = await transformCustomQueries(
      pullUrl,
      mockShard,
      headerOptions,
      mockQueries,
    );

    expect(result).toEqual([
      {
        query: {
          table: 'users',
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'id'},
            right: {type: 'literal', value: 123},
          },
        },
        hash: 'hash1',
      },
      {
        error: 'app',
        id: 'query2',
        name: 'getPostsByUser',
        details: 'Query syntax error',
      },
    ]);
  });

  test('should return HttpError when fetch response is not ok', async () => {
    const mockErrorResponse = new Response(
      'Bad Request: Invalid query format',
      {
        status: 400,
      },
    );

    mockFetchFromAPIServer.mockResolvedValue(mockErrorResponse);

    const result = await transformCustomQueries(
      pullUrl,
      mockShard,
      headerOptions,
      mockQueries,
    );

    expect(result).toEqual({
      error: 'http',
      status: 400,
      details: 'Bad Request: Invalid query format',
    });
  });

  test('should handle empty queries array', async () => {
    const mockSuccessResponse = new Response(
      JSON.stringify(['transformed', []]),
      {status: 200},
    );

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse);

    const result = await transformCustomQueries(
      pullUrl,
      mockShard,
      headerOptions,
      [],
    );

    expect(mockFetchFromAPIServer).toHaveBeenCalledWith(
      pullUrl,
      mockShard,
      headerOptions,
      undefined,
      ['transform', []],
    );

    expect(result).toEqual([]);
  });
});
