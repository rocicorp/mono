import {
  afterEach,
  beforeEach,
  describe,
  expect,
  type MockedFunction,
  test,
  vi,
} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {
  TransformResponseBody,
  TransformResponseMessage,
} from '../../../zero-protocol/src/custom-queries.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import {
  ProtocolError,
  type TransformFailedBody,
} from '../../../zero-protocol/src/error.ts';
import type {TransformedAndHashed} from '../auth/read-authorizer.ts';
import {compileUrlPattern, fetchFromAPIServer} from '../custom/fetch.ts';
import type {CustomQueryRecord} from '../services/view-syncer/schema/types.ts';
import type {ShardID} from '../types/shards.ts';
import {CustomQueryTransformer} from './transform-query.ts';

// Mock the fetch functions
vi.mock('../custom/fetch.ts');
const mockFetchFromAPIServer = fetchFromAPIServer as MockedFunction<
  typeof fetchFromAPIServer
>;
const mockCompileUrlPattern = compileUrlPattern as MockedFunction<
  typeof compileUrlPattern
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

  // Helper to match URLPattern that matches a specific URL
  const expectUrlPatternMatching = (expectedUrl: string) =>
    expect.objectContaining({
      protocol: new URL(expectedUrl).protocol.slice(0, -1), // Remove trailing ':'
      hostname: new URL(expectedUrl).hostname,
      pathname:
        new URL(expectedUrl).pathname === '/'
          ? '*'
          : new URL(expectedUrl).pathname,
    });

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
    mockCompileUrlPattern.mockReturnValue(
      new URLPattern('https://api.example.com/pull'),
    );
    vi.clearAllTimers();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test('should transform queries successfully and return TransformedAndHashed array', async () => {
    mockFetchFromAPIServer.mockResolvedValue([
      'transformed',
      mockQueryResponses,
    ] satisfies TransformResponseMessage);

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
      expect.anything(),
      'transform',
      lc,
      pullUrl,
      false,
      [expectUrlPatternMatching(pullUrl)],
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
    mockFetchFromAPIServer.mockResolvedValue([
      'transformed',
      [
        mockQueryResponses[0],
        {
          error: 'app',
          id: 'query2',
          name: 'getPostsByUser',
          message: 'Query syntax error',
          details: {reason: 'syntax error'},
        },
      ],
    ] satisfies TransformResponseMessage);

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
        message: 'Query syntax error',
        details: {reason: 'syntax error'},
      },
    ]);
  });

  test('should return TransformFailedBody when fetch response is not ok', async () => {
    // HTTP errors now throw ProtocolError from fetchFromAPIServer
    const httpError = new ProtocolError({
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 400,
      bodyPreview: 'Bad Request: Invalid query format',
      message: 'Fetch from API server returned non-OK status 400',
      queryIDs: [],
    });

    mockFetchFromAPIServer.mockRejectedValue(httpError);

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

    // Should return TransformFailedBody with queryIDs filled in
    expect(result).toEqual({
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 400,
      bodyPreview: 'Bad Request: Invalid query format',
      message: 'Fetch from API server returned non-OK status 400',
      queryIDs: ['query1', 'query2'],
    });
  });

  test('should handle empty queries array', async () => {
    mockFetchFromAPIServer.mockResolvedValue(['transformed', []]);

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
      [
        'transformed',
        [mockQueryResponses[0]],
      ] satisfies TransformResponseMessage;

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
      [
        'transformed',
        [mockQueryResponses[0]],
      ] satisfies TransformResponseMessage;

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
      [
        'transformed',
        [mockQueryResponses[0]],
      ] satisfies TransformResponseMessage;

    const mockResponse2 = () =>
      [
        'transformed',
        [mockQueryResponses[1]],
      ] satisfies TransformResponseMessage;

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
      expect.anything(),
      'transform',
      lc,
      'https://api.example.com/pull',
      false,
      [expectUrlPatternMatching('https://api.example.com/pull')],
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
      expect.anything(),
      'transform',
      lc,
      pullUrl,
      false,
      [expectUrlPatternMatching(pullUrl)],
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
      [
        'transformed',
        [mockQueryResponses[0]],
      ] satisfies TransformResponseMessage;

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
      expect.anything(),
      'transform',
      lc,
      pullUrl,
      false,
      [expectUrlPatternMatching(pullUrl)],
      mockShard,
      headerOptions, // Cookies should not be forwarded
      ['transform', [{id: 'query1', name: 'getUserById', args: [123]}]],
    );
    expect(result).toEqual([transformResults[0]]);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);
  });

  test('should forward cookies if forwardCookies is true', async () => {
    const mockSuccessResponse = () =>
      [
        'transformed',
        [mockQueryResponses[0]],
      ] satisfies TransformResponseMessage;

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
      expect.anything(),
      'transform',
      lc,
      pullUrl,
      false,
      [expectUrlPatternMatching(pullUrl)],
      mockShard,
      {...headerOptions, cookie: 'test-cookie'}, // Cookies should be forwarded
      ['transform', [{id: 'query1', name: 'getUserById', args: [123]}]],
    );
    expect(result).toEqual([transformResults[0]]);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);
  });

  test('should not cache error responses', async () => {
    const mockErrorResponse = () =>
      [
        'transformed',
        [
          {
            error: 'app',
            id: 'query1',
            name: 'getUserById',
            message: 'Query syntax error',
            details: {reason: 'Query syntax error'},
          },
        ],
      ] satisfies TransformResponseMessage;

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
        message: 'Query syntax error',
        details: {reason: 'Query syntax error'},
      },
    ]);

    // Second call - should fetch again because errors are not cached
    mockFetchFromAPIServer.mockResolvedValue(mockErrorResponse());
    await transformer.transform(headerOptions, [mockQueries[0]], undefined);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(2);
  });

  test('should use cache key based on header options and query id', async () => {
    const mockSuccessResponse = () =>
      [
        'transformed',
        [mockQueryResponses[0]],
      ] satisfies TransformResponseMessage;

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

    const mockSuccessResponse = () =>
      [
        'transformed',
        [mockQueryResponses[0]],
      ] satisfies TransformResponseMessage;

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );

    const userQueryURL = customUrl;

    await transformer.transform(headerOptions, [mockQueries[0]], userQueryURL);

    // Verify custom URL was used instead of default
    expect(mockFetchFromAPIServer).toHaveBeenCalledWith(
      expect.anything(),
      'transform',
      lc,
      customUrl,
      true,
      [expectUrlPatternMatching(pullUrl)], // Pattern still compiled from config
      mockShard,
      headerOptions,
      ['transform', [{id: 'query1', name: 'getUserById', args: [123]}]],
    );
  });

  test('should use default URL when userQueryURL is undefined', async () => {
    const mockSuccessResponse = () =>
      [
        'transformed',
        [mockQueryResponses[0]],
      ] satisfies TransformResponseMessage;

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );

    await transformer.transform(headerOptions, [mockQueries[0]], undefined);

    // Verify default URL from config was used
    expect(mockFetchFromAPIServer).toHaveBeenCalledWith(
      expect.anything(),
      'transform',
      lc,
      pullUrl,
      false,
      [expectUrlPatternMatching(pullUrl)],
      mockShard,
      headerOptions,
      ['transform', [{id: 'query1', name: 'getUserById', args: [123]}]],
    );
  });

  test('should reject disallowed custom URL', async () => {
    const disallowedUrl = 'https://malicious.com/endpoint';

    // fetchFromAPIServer will throw a regular Error (not ProtocolError) for disallowed URLs
    mockFetchFromAPIServer.mockRejectedValue(
      new Error(
        `URL "${disallowedUrl}" is not allowed by the ZERO_MUTATE/QUERY_URL configuration`,
      ),
    );

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
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

    // Should return TransformFailedBody with the error message
    expect(result).toEqual({
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Internal,
      message: expect.stringContaining(
        `URL "${disallowedUrl}" is not allowed by the ZERO_MUTATE/QUERY_URL configuration`,
      ),
      queryIDs: ['query1'],
    });

    // Verify the disallowed URL was attempted to be used
    expect(mockFetchFromAPIServer).toHaveBeenCalledWith(
      expect.anything(),
      'transform',
      lc,
      disallowedUrl,
      true,
      [expectUrlPatternMatching(pullUrl)], // Pattern still compiled from config
      mockShard,
      headerOptions,
      ['transform', [{id: 'query1', name: 'getUserById', args: [123]}]],
    );
  });

  test('should handle ProtocolError with TransformFailed kind', async () => {
    const protocolError = new ProtocolError({
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Timeout,
      message: 'Request timed out',
      queryIDs: [], // Will be overridden with actual queryIDs
    });

    mockFetchFromAPIServer.mockRejectedValue(protocolError);

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );

    // Should return TransformFailedBody with queryIDs filled in
    const result = await transformer.transform(
      headerOptions,
      [mockQueries[0]],
      undefined,
    );

    expect(result).toEqual({
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Timeout,
      message: 'Request timed out',
      queryIDs: ['query1'],
    });

    // Verify the API was called
    expect(mockFetchFromAPIServer).toHaveBeenCalledWith(
      expect.anything(),
      'transform',
      lc,
      pullUrl,
      false,
      [expectUrlPatternMatching(pullUrl)],
      mockShard,
      headerOptions,
      ['transform', [{id: 'query1', name: 'getUserById', args: [123]}]],
    );
  });

  test('should convert non-ProtocolError exceptions to error responses', async () => {
    const genericError = new Error('Network timeout');

    mockFetchFromAPIServer.mockRejectedValue(genericError);

    const transformer = new CustomQueryTransformer(
      lc,
      {
        url: [pullUrl],
        forwardCookies: false,
      },
      mockShard,
    );

    // This should NOT throw, but return error responses
    const result = await transformer.transform(
      headerOptions,
      [mockQueries[0]],
      undefined,
    );

    // Verify it returns an error response instead of throwing
    expect(result).toEqual({
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Internal,
      message: expect.stringContaining('Network timeout'),
      queryIDs: ['query1'],
    });

    // Verify the API was called
    expect(mockFetchFromAPIServer).toHaveBeenCalledWith(
      expect.anything(),
      'transform',
      lc,
      pullUrl,
      false,
      [expectUrlPatternMatching(pullUrl)],
      mockShard,
      headerOptions,
      ['transform', [{id: 'query1', name: 'getUserById', args: [123]}]],
    );
  });

  test('should pass through transformFailed response from API server', async () => {
    // API server returns 200 OK but with a transformFailed message
    const transformFailedBody: TransformFailedBody = {
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Parse,
      message: 'Unable to transform query due to invalid schema',
      queryIDs: ['query1', 'query2'],
    };

    mockFetchFromAPIServer.mockResolvedValue([
      'transformFailed',
      transformFailedBody,
    ] satisfies TransformResponseMessage);

    const transformer = new CustomQueryTransformer(
      lc,
      {url: [pullUrl], forwardCookies: false},
      mockShard,
    );

    const result = await transformer.transform(
      headerOptions,
      mockQueries,
      undefined,
    );

    // Should return transformFailedBody when transformFailed response is received
    expect(result).toEqual(transformFailedBody);
  });

  test('should handle non-Error exceptions', async () => {
    mockFetchFromAPIServer.mockRejectedValue('string error thrown');

    const transformer = new CustomQueryTransformer(
      lc,
      {url: [pullUrl], forwardCookies: false},
      mockShard,
    );

    const result = await transformer.transform(
      headerOptions,
      [mockQueries[0]],
      undefined,
    );

    expect(result).toEqual({
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Internal,
      message: expect.stringContaining('string error thrown'),
      queryIDs: ['query1'],
    });
  });
});
