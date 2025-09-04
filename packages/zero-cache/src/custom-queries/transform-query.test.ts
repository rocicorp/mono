import {
  describe,
  expect,
  vi,
  beforeEach,
  afterEach,
  type MockedFunction,
  test,
} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {CustomQueryTransformer} from './transform-query.ts';
import {fetchFromAPIServer} from '../custom/fetch.ts';
import type {CustomQueryRecord} from '../services/view-syncer/schema/types.ts';
import type {ShardID} from '../types/shards.ts';
import type {
  TransformResponseMessage,
  TransformResponseBody,
} from '../../../zero-protocol/src/custom-queries.ts';
import type {TransformedAndHashed} from '../auth/read-authorizer.ts';

// Mock the fetchFromAPIServer function
vi.mock('../custom/fetch.ts');
const mockFetchFromAPIServer = fetchFromAPIServer as MockedFunction<
  typeof fetchFromAPIServer
>;

describe('CustomQueryTransformer', () => {
  const mockShard: ShardID = {
    appID: 'test_app',
    shardNum: 1,
  };
  const lc = createSilentLogContext();

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

  const mockQueryResponses: TransformResponseBody = [
    {
      id: 'query1',
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
      id: 'query2',
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
  ];

  const transformResults: TransformedAndHashed[] = [
    {
      id: 'query1',
      transformedAst: {
        table: 'users',
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: 123},
        },
      },
      transformationHash: '2q4jya9umt1i2',
    },
    {
      id: 'query2',
      transformedAst: {
        table: 'posts',
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'userId'},
          right: {type: 'literal', value: 'user123'},
        },
      },
      transformationHash: 'ofy7rz1vol9y',
    },
  ];

  beforeEach(() => {
    mockFetchFromAPIServer.mockReset();
    vi.clearAllTimers();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test('should transform queries successfully and return TransformedAndHashed array', async () => {
    const mockSuccessResponse = new Response(
      JSON.stringify([
        'transformed',
        mockQueryResponses,
      ] satisfies TransformResponseMessage),
      {status: 200},
    );

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse);

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );
    const result = await transformer.transform(
      headerOptions,
      mockQueries,
      undefined,
    );

    // Verify the API was called correctly
    expect(mockFetchFromAPIServer).toHaveBeenCalledWith(
      lc,
      pullUrl,
      [pullUrl],
      mockShard,
      headerOptions,
      [
        'transform',
        [
          {id: 'query1', name: 'getUserById', args: [123]},
          {id: 'query2', name: 'getPostsByUser', args: ['user123', 10]},
        ],
      ],
    );

    // Verify the result
    expect(result).toEqual(transformResults);
  });

  test('should handle errored queries in response', async () => {
    const mockMixedResponse = new Response(
      JSON.stringify([
        'transformed',
        [
          mockQueryResponses[0],
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

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );
    const result = await transformer.transform(
      headerOptions,
      mockQueries,
      undefined,
    );

    expect(result).toEqual([
      transformResults[0],
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

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );
    const result = await transformer.transform(
      headerOptions,
      mockQueries,
      undefined,
    );

    expect(result).toEqual([
      {
        details: 'Bad Request: Invalid query format',
        error: 'http',
        id: 'query1',
        name: 'getUserById',
        status: 400,
      },
      {
        details: 'Bad Request: Invalid query format',
        error: 'http',
        id: 'query2',
        name: 'getPostsByUser',
        status: 400,
      },
    ]);
  });

  test('should handle empty queries array', async () => {
    const mockSuccessResponse = new Response(
      JSON.stringify(['transformed', []]),
      {status: 200},
    );

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse);

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );
    const result = await transformer.transform(headerOptions, [], undefined);

    expect(mockFetchFromAPIServer).not.toHaveBeenCalled();
    expect(result).toEqual([]);
  });

  test('should not fetch cached responses', async () => {
    const mockSuccessResponse = () =>
      new Response(
        JSON.stringify([
          'transformed',
          [mockQueryResponses[0]],
        ] satisfies TransformResponseMessage),
        {status: 200},
      );

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );

    // First call - should fetch
    await transformer.transform(headerOptions, [mockQueries[0]], undefined);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);

    // Second call with same query - should use cache, not fetch
    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());
    const result = await transformer.transform(
      headerOptions,
      [mockQueries[0]],
      undefined,
    );
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1); // Still only called once
    expect(result).toEqual([transformResults[0]]);
  });

  test('should cache successful responses for 5 seconds', async () => {
    const mockSuccessResponse = () =>
      new Response(
        JSON.stringify([
          'transformed',
          [mockQueryResponses[0]],
        ] satisfies TransformResponseMessage),
        {status: 200},
      );

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );

    // First call
    await transformer.transform(headerOptions, [mockQueries[0]], undefined);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);

    // Advance time by 4 seconds - should still use cache
    vi.advanceTimersByTime(4000);
    await transformer.transform(headerOptions, [mockQueries[0]], undefined);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);

    // Advance time by 2 more seconds (6 total) - cache should expire, fetch again
    vi.advanceTimersByTime(2000);
    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());
    await transformer.transform(headerOptions, [mockQueries[0]], undefined);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(2);
  });

  test('should handle mixed cached and uncached queries', async () => {
    const mockResponse1 = () =>
      new Response(
        JSON.stringify([
          'transformed',
          [mockQueryResponses[0]],
        ] satisfies TransformResponseMessage),
        {status: 200},
      );

    const mockResponse2 = () =>
      new Response(
        JSON.stringify([
          'transformed',
          [mockQueryResponses[1]],
        ] satisfies TransformResponseMessage),
        {status: 200},
      );

    mockFetchFromAPIServer
      .mockResolvedValueOnce(mockResponse1())
      .mockResolvedValueOnce(mockResponse2());

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );

    // Cache first query
    await transformer.transform(headerOptions, [mockQueries[0]], undefined);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);
    expect(mockFetchFromAPIServer).toHaveBeenLastCalledWith(
      lc,
      'https://api.example.com/pull',
      ['https://api.example.com/pull'],
      mockShard,
      headerOptions,
      ['transform', [{id: 'query1', name: 'getUserById', args: [123]}]],
    );

    // Now call with both queries - only second should be fetched
    const result = await transformer.transform(
      headerOptions,
      mockQueries,
      undefined,
    );
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(2);
    expect(mockFetchFromAPIServer).toHaveBeenLastCalledWith(
      lc,
      pullUrl,
      [pullUrl],
      mockShard,
      headerOptions,
      [
        'transform',
        [{id: 'query2', name: 'getPostsByUser', args: ['user123', 10]}],
      ],
    );

    // Verify combined result includes both cached and fresh data
    expect(result).toHaveLength(2);
    expect(result).toEqual(expect.arrayContaining(transformResults));
  });

  test('should not forward cookies if forwardCookies is false', async () => {
    const mockSuccessResponse = () =>
      new Response(
        JSON.stringify([
          'transformed',
          [mockQueryResponses[0]],
        ] satisfies TransformResponseMessage),
        {status: 200},
      );

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );

    // Call with cookies in header options
    const result = await transformer.transform(
      {...headerOptions, cookie: 'test-cookie'},
      [mockQueries[0]],
      undefined,
    );

    expect(mockFetchFromAPIServer).toHaveBeenCalledWith(
      lc,
      pullUrl,
      [pullUrl],
      mockShard,
      headerOptions, // Cookies should not be forwarded
      ['transform', [{id: 'query1', name: 'getUserById', args: [123]}]],
    );
    expect(result).toEqual([transformResults[0]]);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);
  });

  test('should forward cookies if forwardCookies is true', async () => {
    const mockSuccessResponse = () =>
      new Response(
        JSON.stringify([
          'transformed',
          [mockQueryResponses[0]],
        ] satisfies TransformResponseMessage),
        {status: 200},
      );

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: true,
      },
      mockShard,
    );

    // Call with cookies in header options
    const result = await transformer.transform(
      {...headerOptions, cookie: 'test-cookie'},
      [mockQueries[0]],
      undefined,
    );

    expect(mockFetchFromAPIServer).toHaveBeenCalledWith(
      lc,
      pullUrl,
      [pullUrl],
      mockShard,
      {...headerOptions, cookie: 'test-cookie'}, // Cookies should be forwarded
      ['transform', [{id: 'query1', name: 'getUserById', args: [123]}]],
    );
    expect(result).toEqual([transformResults[0]]);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);
  });

  test('should not cache error responses', async () => {
    const mockErrorResponse = () =>
      new Response(
        JSON.stringify([
          'transformed',
          [
            {
              error: 'app',
              id: 'query1',
              name: 'getUserById',
              details: 'Query syntax error',
            },
          ],
        ] satisfies TransformResponseMessage),
        {status: 200},
      );

    mockFetchFromAPIServer.mockResolvedValue(mockErrorResponse());

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );

    // First call - should fetch and get error
    const result1 = await transformer.transform(
      headerOptions,
      [mockQueries[0]],
      undefined,
    );
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);
    expect(result1).toEqual([
      {
        error: 'app',
        id: 'query1',
        name: 'getUserById',
        details: 'Query syntax error',
      },
    ]);

    // Second call - should fetch again because errors are not cached
    mockFetchFromAPIServer.mockResolvedValue(mockErrorResponse());
    await transformer.transform(headerOptions, [mockQueries[0]], undefined);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(2);
  });

  test('should use cache key based on header options and query id', async () => {
    const mockSuccessResponse = () =>
      new Response(
        JSON.stringify([
          'transformed',
          [mockQueryResponses[0]],
        ] satisfies TransformResponseMessage),
        {status: 200},
      );

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );
    const differentHeaderOptions = {
      apiKey: 'different-api-key',
      token: 'different-token',
    };

    // Cache with first header options
    await transformer.transform(headerOptions, [mockQueries[0]], undefined);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);

    // Call with different header options - should fetch again due to different cache key
    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());
    await transformer.transform(
      differentHeaderOptions,
      [mockQueries[0]],
      undefined,
    );
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(2);

    // Call again with original header options - should use cache
    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());
    await transformer.transform(headerOptions, [mockQueries[0]], undefined);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(2);
  });

  test('should use custom URL when userQueryURL is provided', async () => {
    const customUrl = 'https://custom-api.example.com/transform';
    const defaultUrl = 'https://default-api.example.com/transform';

    const mockSuccessResponse = () =>
      new Response(
        JSON.stringify([
          'transformed',
          [mockQueryResponses[0]],
        ] satisfies TransformResponseMessage),
        {status: 200},
      );

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [defaultUrl, customUrl],
        forwardCookies: false,
      },
      mockShard,
    );

    const userQueryURL = customUrl;

    await transformer.transform(
      headerOptions,
      [mockQueries[0]],
      userQueryURL,
    );

    // Verify custom URL was used instead of default
    expect(mockFetchFromAPIServer).toHaveBeenCalledWith(
      lc,
      customUrl,
      [defaultUrl, customUrl],
      mockShard,
      headerOptions,
      ['transform', [{id: 'query1', name: 'getUserById', args: [123]}]],
    );
  });

  test('should use default URL when userQueryURL is undefined', async () => {
    const defaultUrl = 'https://default-api.example.com/transform';

    const mockSuccessResponse = () =>
      new Response(
        JSON.stringify([
          'transformed',
          [mockQueryResponses[0]],
        ] satisfies TransformResponseMessage),
        {status: 200},
      );

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [defaultUrl],
        forwardCookies: false,
      },
      mockShard,
    );

    await transformer.transform(headerOptions, [mockQueries[0]], undefined);

    // Verify default URL was used
    expect(mockFetchFromAPIServer).toHaveBeenCalledWith(
      lc,
      defaultUrl,
      [defaultUrl],
      mockShard,
      headerOptions,
      ['transform', [{id: 'query1', name: 'getUserById', args: [123]}]],
    );
  });

  test('should reject disallowed custom URL', async () => {
    const allowedUrl = 'https://allowed-api.example.com/transform';
    const disallowedUrl = 'https://malicious.com/endpoint';

    mockFetchFromAPIServer.mockRejectedValue(
      new Error(
        `URL "${disallowedUrl}" is not allowed by the ZERO_MUTATE/GET_QUERIES_URL configuration`,
      ),
    );

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [allowedUrl],
        forwardCookies: false,
      },
      mockShard,
    );

    const userQueryURL = disallowedUrl;

    const result = await transformer.transform(
      headerOptions,
      [mockQueries[0]],
      userQueryURL,
    );

    // Verify the disallowed URL caused an error
    expect(result).toEqual([
      {
        error: 'zero',
        details: `URL "${disallowedUrl}" is not allowed by the ZERO_MUTATE/GET_QUERIES_URL configuration`,
        id: 'query1',
        name: 'getUserById',
      },
    ]);

    // Verify the disallowed URL was attempted to be used
    expect(mockFetchFromAPIServer).toHaveBeenCalledWith(
      lc,
      disallowedUrl,
      [allowedUrl],
      mockShard,
      headerOptions,
      ['transform', [{id: 'query1', name: 'getUserById', args: [123]}]],
    );
  });
});
