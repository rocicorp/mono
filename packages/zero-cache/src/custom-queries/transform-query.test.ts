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
import {fetchFromAPIServer} from '../custom/fetch.ts';
import type {
  ConnectionContext,
  HeaderOptions,
} from '../services/view-syncer/connection-context-manager.ts';
import type {CustomQueryRecord} from '../services/view-syncer/schema/types.ts';
import type {ShardID} from '../types/shards.ts';
import {CustomQueryTransformer} from './transform-query.ts';

// Mock the fetch functions
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
  const defaultAuth = 'test-token';

  const pullUrl = 'https://api.example.com/pull';
  const headerOptions: HeaderOptions = {
    apiKey: 'test-api-key',
  };

  function makeContext(
    options: {
      headerOptions?: HeaderOptions | undefined;
      userQueryURL?: string | undefined;
      forwardCookies?: boolean | undefined;
      auth?: string | undefined;
      userID?: string | undefined;
    } = {},
  ): ConnectionContext {
    const normalizedHeaderOptions: HeaderOptions = options.forwardCookies
      ? (options.headerOptions ?? headerOptions)
      : {...(options.headerOptions ?? headerOptions), cookie: undefined};
    const auth = options.auth ?? defaultAuth;

    return {
      state: 'provisional',
      clientID: 'test-client',
      wsID: 'test-ws',
      userID: options.userID,
      auth: auth ? {type: 'opaque', raw: auth} : undefined,
      profileID: null,
      baseCookie: null,
      protocolVersion: 0,
      revision: 0,
      revalidateAt: undefined,
      insertionOrder: 0,
      queryContext: {
        url: options.userQueryURL ?? pullUrl,
        allowedUrlPatterns: [new URLPattern(pullUrl)],
        headerOptions: normalizedHeaderOptions,
      },
      pushContext: {
        url: undefined,
        allowedUrlPatterns: [],
        headerOptions: {
          apiKey: undefined,
          allowedClientHeaders: undefined,
          customHeaders: undefined,
          cookie: undefined,
          origin: undefined,
        },
      },
    };
  }

  function makeTransformer(
    options: {forwardCookies?: boolean | undefined} = {},
  ) {
    const transformer = new CustomQueryTransformer(lc, mockShard);
    return {
      transform(
        inputHeaderOptions: HeaderOptions,
        queries: Iterable<CustomQueryRecord>,
        userQueryURL: string | undefined,
        contextOptions: {
          auth?: string | undefined;
          userID?: string | undefined;
        } = {},
      ) {
        return transformer.transform(
          makeContext({
            headerOptions: inputHeaderOptions,
            userQueryURL,
            forwardCookies: options.forwardCookies,
            ...contextOptions,
          }),
          queries,
        );
      },

      validate(
        inputHeaderOptions: HeaderOptions,
        userQueryURL: string | undefined,
        contextOptions: {
          auth?: string | undefined;
          userID?: string | undefined;
        } = {},
      ) {
        return transformer.validate(
          makeContext({
            headerOptions: inputHeaderOptions,
            userQueryURL,
            forwardCookies: options.forwardCookies,
            ...contextOptions,
          }),
        );
      },
    };
  }

  function expectContext(
    options: {
      headerOptions?: HeaderOptions | undefined;
      userQueryURL?: string | undefined;
      forwardCookies?: boolean | undefined;
      auth?: string | undefined;
      userID?: string | undefined;
    } = {},
  ) {
    const normalizedHeaderOptions: HeaderOptions = options.forwardCookies
      ? (options.headerOptions ?? headerOptions)
      : {...(options.headerOptions ?? headerOptions), cookie: undefined};
    const auth = options.auth ?? defaultAuth;

    return expect.objectContaining({
      userID: options.userID,
      auth: auth ? {type: 'opaque', raw: auth} : undefined,
      queryContext: expect.objectContaining({
        url: options.userQueryURL ?? pullUrl,
        allowedUrlPatterns: expect.arrayContaining([
          expectUrlPatternMatching(pullUrl),
        ]),
        headerOptions: normalizedHeaderOptions,
      }),
    });
  }

  function transformRequest(queries: Iterable<CustomQueryRecord>) {
    return [
      'transform',
      Array.from(queries, ({id, name, args}) => ({id, name, args})),
    ] as const;
  }

  function transformedMessage(
    body: TransformResponseBody,
  ): TransformResponseMessage {
    return ['transformed', body];
  }

  function transformFailedMessage(
    body: TransformFailedBody,
  ): TransformResponseMessage {
    return ['transformFailed', body];
  }

  function expectLastTransformFetch(
    queries: Iterable<CustomQueryRecord>,
    options: Parameters<typeof expectContext>[0] = {},
  ) {
    expect(mockFetchFromAPIServer).toHaveBeenLastCalledWith(
      expect.anything(),
      'transform',
      lc,
      expectContext(options),
      mockShard,
      transformRequest(queries),
    );
  }

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

  type TransformAttempt = Awaited<
    ReturnType<ReturnType<typeof makeTransformer>['transform']>
  >;

  function expectTransformAttempt(
    actual: TransformAttempt,
    expected: TransformAttempt['result'],
    cached = false,
  ) {
    expect(actual).toEqual({result: expected, cached});
  }

  beforeEach(() => {
    mockFetchFromAPIServer.mockReset();
    vi.clearAllTimers();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test('should transform queries successfully and return TransformedAndHashed array', async () => {
    mockFetchFromAPIServer.mockResolvedValue(
      transformedMessage(mockQueryResponses),
    );

    const transformer = makeTransformer();
    const result = await transformer.transform(
      headerOptions,
      mockQueries,
      undefined,
    );

    // Verify the API was called correctly
    expectLastTransformFetch(mockQueries);

    // Verify the result
    expectTransformAttempt(result, transformResults);
  });

  test('validate should hit the API with an empty transform request', async () => {
    mockFetchFromAPIServer.mockResolvedValue(transformedMessage([]));

    const transformer = makeTransformer();
    const result = await transformer.validate(headerOptions, undefined);

    expectLastTransformFetch([]);
    expect(result).toBeUndefined();
  });

  test('validate should pass through transformFailed responses', async () => {
    const transformFailedBody: TransformFailedBody = {
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Parse,
      message: 'Unable to validate query credentials',
      queryIDs: [],
    };

    mockFetchFromAPIServer.mockResolvedValue(
      transformFailedMessage(transformFailedBody),
    );

    const transformer = makeTransformer();
    const result = await transformer.validate(headerOptions, undefined);

    expect(result).toEqual(transformFailedBody);
  });

  test('should handle errored queries in response', async () => {
    mockFetchFromAPIServer.mockResolvedValue(
      transformedMessage([
        mockQueryResponses[0],
        {
          error: 'app',
          id: 'query2',
          name: 'getPostsByUser',
          message: 'Query syntax error',
          details: {reason: 'syntax error'},
        },
      ]),
    );

    const transformer = makeTransformer();
    const result = await transformer.transform(
      headerOptions,
      mockQueries,
      undefined,
    );

    expectTransformAttempt(result, [
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

    const transformer = makeTransformer();
    const result = await transformer.transform(
      headerOptions,
      mockQueries,
      undefined,
    );

    // Should return TransformFailedBody with queryIDs filled in
    expectTransformAttempt(result, {
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
    mockFetchFromAPIServer.mockResolvedValue(transformedMessage([]));

    const transformer = makeTransformer();
    const result = await transformer.transform(headerOptions, [], undefined);

    expect(mockFetchFromAPIServer).not.toHaveBeenCalled();
    expectTransformAttempt(result, [], true);
  });

  test('should not fetch cached responses', async () => {
    const mockSuccessResponse = () =>
      transformedMessage([mockQueryResponses[0]]);

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = makeTransformer();

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
    expectTransformAttempt(result, [transformResults[0]], true);
  });

  test('should cache successful responses for 5 seconds', async () => {
    const mockSuccessResponse = () =>
      transformedMessage([mockQueryResponses[0]]);

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = makeTransformer();

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
    const mockResponse1 = () => transformedMessage([mockQueryResponses[0]]);

    const mockResponse2 = () => transformedMessage([mockQueryResponses[1]]);

    mockFetchFromAPIServer
      .mockResolvedValueOnce(mockResponse1())
      .mockResolvedValueOnce(mockResponse2());

    const transformer = makeTransformer();

    // Cache first query
    await transformer.transform(headerOptions, [mockQueries[0]], undefined);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);
    expectLastTransformFetch([mockQueries[0]]);

    // Now call with both queries - only second should be fetched
    const result = await transformer.transform(
      headerOptions,
      mockQueries,
      undefined,
    );
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(2);
    expectLastTransformFetch([mockQueries[1]]);

    // Verify combined result includes both cached and fresh data
    expect(result.cached).toBe(false);
    expect(result.result).toHaveLength(2);
    expect(result.result).toEqual(expect.arrayContaining(transformResults));
  });

  test('should not forward cookies if forwardCookies is false', async () => {
    const mockSuccessResponse = () =>
      transformedMessage([mockQueryResponses[0]]);

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = makeTransformer();

    // Call with cookies in header options
    const result = await transformer.transform(
      {...headerOptions, cookie: 'test-cookie'},
      [mockQueries[0]],
      undefined,
    );

    expectLastTransformFetch([mockQueries[0]], {
      headerOptions: {...headerOptions, cookie: 'test-cookie'},
    });
    expectTransformAttempt(result, [transformResults[0]]);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);
  });

  test('should forward cookies if forwardCookies is true', async () => {
    const mockSuccessResponse = () =>
      transformedMessage([mockQueryResponses[0]]);

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = makeTransformer({forwardCookies: true});

    // Call with cookies in header options
    const result = await transformer.transform(
      {...headerOptions, cookie: 'test-cookie'},
      [mockQueries[0]],
      undefined,
    );

    expectLastTransformFetch([mockQueries[0]], {
      headerOptions: {...headerOptions, cookie: 'test-cookie'},
      forwardCookies: true,
    });
    expectTransformAttempt(result, [transformResults[0]]);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);
  });

  test('should not cache error responses', async () => {
    const mockErrorResponse = () =>
      transformedMessage([
        {
          error: 'app',
          id: 'query1',
          name: 'getUserById',
          message: 'Query syntax error',
          details: {reason: 'Query syntax error'},
        },
      ]);

    mockFetchFromAPIServer.mockResolvedValue(mockErrorResponse());

    const transformer = makeTransformer();

    // First call - should fetch and get error
    const result1 = await transformer.transform(
      headerOptions,
      [mockQueries[0]],
      undefined,
    );
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);
    expectTransformAttempt(result1, [
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

  test('should use cache key based on auth context and query id', async () => {
    const mockSuccessResponse = () =>
      transformedMessage([mockQueryResponses[0]]);

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = makeTransformer();
    // Cache with first header options
    await transformer.transform(headerOptions, [mockQueries[0]], undefined);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(1);

    // Call with different auth - should fetch again due to different cache key
    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());
    await transformer.transform(headerOptions, [mockQueries[0]], undefined, {
      auth: 'different-token',
    });
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(2);

    // Call again with original header options - should use cache
    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());
    await transformer.transform(headerOptions, [mockQueries[0]], undefined);
    expect(mockFetchFromAPIServer).toHaveBeenCalledTimes(2);
  });

  test('should use custom URL when userQueryURL is provided', async () => {
    const customUrl = 'https://custom-api.example.com/transform';

    const mockSuccessResponse = () =>
      transformedMessage([mockQueryResponses[0]]);

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = makeTransformer();

    const userQueryURL = customUrl;

    await transformer.transform(headerOptions, [mockQueries[0]], userQueryURL);

    // Verify custom URL was used instead of default
    expectLastTransformFetch([mockQueries[0]], {userQueryURL: customUrl});
  });

  test('should use default URL when userQueryURL is undefined', async () => {
    const mockSuccessResponse = () =>
      transformedMessage([mockQueryResponses[0]]);

    mockFetchFromAPIServer.mockResolvedValue(mockSuccessResponse());

    const transformer = makeTransformer();

    await transformer.transform(headerOptions, [mockQueries[0]], undefined);

    // Verify default URL from config was used
    expectLastTransformFetch([mockQueries[0]]);
  });

  test('should reject disallowed custom URL', async () => {
    const disallowedUrl = 'https://malicious.com/endpoint';

    // fetchFromAPIServer will throw a regular Error (not ProtocolError) for disallowed URLs
    mockFetchFromAPIServer.mockRejectedValue(
      new Error(
        `URL "${disallowedUrl}" is not allowed by the ZERO_MUTATE/QUERY_URL configuration`,
      ),
    );

    const transformer = makeTransformer();

    const userQueryURL = disallowedUrl;

    const result = await transformer.transform(
      headerOptions,
      [mockQueries[0]],
      userQueryURL,
    );

    // Should return TransformFailedBody with the error message
    expectTransformAttempt(result, {
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Internal,
      message: expect.stringContaining(
        `URL "${disallowedUrl}" is not allowed by the ZERO_MUTATE/QUERY_URL configuration`,
      ),
      queryIDs: ['query1'],
    });

    // Verify the disallowed URL was attempted to be used
    expectLastTransformFetch([mockQueries[0]], {userQueryURL: disallowedUrl});
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

    const transformer = makeTransformer();

    // Should return TransformFailedBody with queryIDs filled in
    const result = await transformer.transform(
      headerOptions,
      [mockQueries[0]],
      undefined,
    );

    expectTransformAttempt(result, {
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Timeout,
      message: 'Request timed out',
      queryIDs: ['query1'],
    });

    // Verify the API was called
    expectLastTransformFetch([mockQueries[0]]);
  });

  test('should convert non-ProtocolError exceptions to error responses', async () => {
    const genericError = new Error('Network timeout');

    mockFetchFromAPIServer.mockRejectedValue(genericError);

    const transformer = makeTransformer();

    // This should NOT throw, but return error responses
    const result = await transformer.transform(
      headerOptions,
      [mockQueries[0]],
      undefined,
    );

    // Verify it returns an error response instead of throwing
    expectTransformAttempt(result, {
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Internal,
      message: expect.stringContaining('Network timeout'),
      queryIDs: ['query1'],
    });

    // Verify the API was called
    expectLastTransformFetch([mockQueries[0]]);
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

    mockFetchFromAPIServer.mockResolvedValue(
      transformFailedMessage(transformFailedBody),
    );

    const transformer = makeTransformer();

    const result = await transformer.transform(
      headerOptions,
      mockQueries,
      undefined,
    );

    // Should return transformFailedBody when transformFailed response is received
    expectTransformAttempt(result, transformFailedBody);
  });

  test('should handle non-Error exceptions', async () => {
    mockFetchFromAPIServer.mockRejectedValue('string error thrown');

    const transformer = makeTransformer();

    const result = await transformer.transform(
      headerOptions,
      [mockQueries[0]],
      undefined,
    );

    expectTransformAttempt(result, {
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Internal,
      message: expect.stringContaining('string error thrown'),
      queryIDs: ['query1'],
    });
  });
});
