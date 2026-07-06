import {LogContext} from '@rocicorp/logger';
import {
  afterAll,
  assert,
  beforeEach,
  describe,
  expect,
  test,
  vi,
  type MockedFunction,
} from 'vitest';
import {
  TestLogSink,
  createSilentLogContext,
} from '../../../shared/src/logging-test-utils.ts';
import * as v from '../../../shared/src/valita.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import {
  ProtocolError,
  isProtocolError,
} from '../../../zero-protocol/src/error.ts';
import {mutateResponseSchema} from '../../../zero-protocol/src/mutate-server.ts';
import {queryResponseSchema} from '../../../zero-protocol/src/query-server.ts';
import type {
  ConnectionContext,
  HeaderOptions,
} from '../services/view-syncer/connection-context-manager.ts';
import type {ShardID} from '../types/shards.ts';
import {
  compileUrlPattern,
  fetchFromAPIServer,
  getBodyPreview,
  urlMatch,
} from './fetch.ts';
import * as apiMetrics from './metrics.ts';

const mockFetch = vi.fn() as MockedFunction<typeof fetch>;
vi.stubGlobal('fetch', mockFetch);

const shard: ShardID = {appID: 'test_app', shardNum: 1};
const baseUrl = 'https://api.example.com/endpoint';
const allowedPatterns = [compileUrlPattern(baseUrl)];

function mockApiMetrics() {
  const requestsAdd = vi.fn();
  const requestDurationRecordMs = vi.fn();
  const attemptsAdd = vi.fn();
  const attemptDurationRecordMs = vi.fn();
  const inFlightAdd = vi.fn();

  vi.spyOn(apiMetrics, 'apiRequests').mockReturnValue({
    add: requestsAdd,
  } as unknown as ReturnType<typeof apiMetrics.apiRequests>);
  vi.spyOn(apiMetrics, 'apiRequestDuration').mockReturnValue({
    recordMs: requestDurationRecordMs,
  } as unknown as ReturnType<typeof apiMetrics.apiRequestDuration>);
  vi.spyOn(apiMetrics, 'apiAttempts').mockReturnValue({
    add: attemptsAdd,
  } as unknown as ReturnType<typeof apiMetrics.apiAttempts>);
  vi.spyOn(apiMetrics, 'apiAttemptDuration').mockReturnValue({
    recordMs: attemptDurationRecordMs,
  } as unknown as ReturnType<typeof apiMetrics.apiAttemptDuration>);
  vi.spyOn(apiMetrics, 'apiInFlight').mockReturnValue({
    add: inFlightAdd,
  } as unknown as ReturnType<typeof apiMetrics.apiInFlight>);

  return {
    requestsAdd,
    requestDurationRecordMs,
    attemptsAdd,
    attemptDurationRecordMs,
    inFlightAdd,
  };
}

afterAll(() => {
  vi.unstubAllGlobals();
});

describe('fetchFromAPIServer', () => {
  const lc = createSilentLogContext();
  const body = {test: 'data'};
  const validator = v.object({success: v.boolean()});
  let metrics: ReturnType<typeof mockApiMetrics>;

  type FetchContextOptions = {
    url?: string | undefined;
    allowedUrlPatterns?: URLPattern[] | undefined;
    headerOptions?: HeaderOptions | undefined;
    auth?: string | undefined;
    userID?: string | undefined;
  };

  function makeContext(options: FetchContextOptions = {}): ConnectionContext {
    const url = options.url ?? baseUrl;
    const allowedUrlPatterns = options.allowedUrlPatterns ?? allowedPatterns;
    const headerOptions = options.headerOptions ?? {};

    return {
      state: 'provisional',
      clientID: 'test-client',
      wsID: 'test-ws',
      user: {id: options.userID ?? null},
      auth: options.auth ? {type: 'opaque', raw: options.auth} : undefined,
      profileID: null,
      baseCookie: null,
      protocolVersion: 0,
      revision: 0,
      revalidateAt: undefined,
      insertionOrder: 0,
      queryContext: {
        url,
        allowedUrlPatterns,
        headerOptions,
      },
      mutateContext: {
        url,
        allowedUrlPatterns,
        headerOptions,
      },
    };
  }

  function fetchWithContext<
    TValidator extends Parameters<typeof fetchFromAPIServer>[0],
  >(
    validator: TValidator,
    source: Parameters<typeof fetchFromAPIServer>[1],
    metricsOptions: Parameters<typeof fetchFromAPIServer>[6],
    options: FetchContextOptions = {},
    requestBody: typeof body = body,
  ) {
    return fetchFromAPIServer(
      validator,
      source,
      lc,
      makeContext(options),
      shard,
      requestBody,
      metricsOptions,
    );
  }

  beforeEach(() => {
    vi.restoreAllMocks();
    mockFetch.mockReset();
    metrics = mockApiMetrics();
    vi.useRealTimers();
  });

  test('returns parsed JSON on success and sends expected headers', async () => {
    const responsePayload = {success: true};
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(responsePayload), {status: 200}),
    );

    const result = await fetchWithContext(
      validator,
      'push',
      {operation: 'mutate'},
      {
        headerOptions: {
          apiKey: 'key-123',
          cookie: 'session=xyz',
        },
        auth: 'token-abc',
      },
    );

    expect(result).toEqual(responsePayload);
    const [calledUrl, init] = mockFetch.mock.calls[0]!;
    const url = new URL(calledUrl as string);
    expect(url.origin + url.pathname).toBe(baseUrl);
    expect(url.searchParams.get('schema')).toBe('test_app_1');
    expect(url.searchParams.get('appID')).toBe('test_app');
    expect(init?.method).toBe('POST');
    expect(init?.body).toBe(JSON.stringify(body));
    expect(init?.headers).toEqual({
      'Content-Type': 'application/json',
      'X-Api-Key': 'key-123',
      'Authorization': 'Bearer token-abc',
      'Cookie': 'session=xyz',
    });
  });

  test('records API metrics for successful requests', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true}), {status: 200}),
    );

    await fetchWithContext(validator, 'push', {operation: 'mutate'});

    expect(metrics.inFlightAdd).toHaveBeenNthCalledWith(1, 1, {
      operation: 'mutate',
    });
    expect(metrics.inFlightAdd).toHaveBeenNthCalledWith(2, -1, {
      operation: 'mutate',
    });
    expect(metrics.attemptsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        operation: 'mutate',
        attempt: 1,
        result: 'success',
        will_retry: false,
        http_status_code: 200,
        http_status_class: '2xx',
      }),
    );
    expect(metrics.attemptDurationRecordMs).toHaveBeenCalledWith(
      expect.any(Number),
      expect.objectContaining({result: 'success'}),
    );
    expect(metrics.requestsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        operation: 'mutate',
        result: 'success',
        attempt_count: 1,
        http_status_code: 200,
        http_status_class: '2xx',
      }),
    );
    expect(metrics.requestDurationRecordMs).toHaveBeenCalledWith(
      expect.any(Number),
      expect.objectContaining({result: 'success'}),
    );
  });

  test('records retry attempts and final success', async () => {
    vi.useFakeTimers();
    mockFetch
      .mockResolvedValueOnce(new Response('bad gateway', {status: 502}))
      .mockResolvedValueOnce(new Response('bad gateway', {status: 502}))
      .mockResolvedValueOnce(
        new Response(JSON.stringify({success: true}), {status: 200}),
      );

    const promise = fetchWithContext(validator, 'push', {operation: 'mutate'});

    await vi.advanceTimersByTimeAsync(2400);
    await promise;

    expect(metrics.attemptsAdd).toHaveBeenCalledTimes(3);
    expect(metrics.attemptsAdd).toHaveBeenNthCalledWith(
      1,
      1,
      expect.objectContaining({
        attempt: 1,
        result: 'http_error',
        will_retry: true,
        http_status_code: 502,
      }),
    );
    expect(metrics.attemptsAdd).toHaveBeenNthCalledWith(
      2,
      1,
      expect.objectContaining({
        attempt: 2,
        result: 'http_error',
        will_retry: true,
        http_status_code: 502,
      }),
    );
    expect(metrics.attemptsAdd).toHaveBeenNthCalledWith(
      3,
      1,
      expect.objectContaining({
        attempt: 3,
        result: 'success',
        will_retry: false,
        http_status_code: 200,
      }),
    );
    expect(metrics.requestsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        result: 'success',
        attempt_count: 3,
      }),
    );
  });

  test('records exhausted HTTP retries as http_error', async () => {
    vi.useFakeTimers();
    mockFetch.mockResolvedValue(new Response('bad gateway', {status: 502}));

    const promise = fetchWithContext(validator, 'push', {operation: 'mutate'});

    await Promise.all([
      expect(promise).rejects.toThrow(/non-OK status 502/),
      vi.advanceTimersByTimeAsync(5000),
    ]);

    expect(metrics.attemptsAdd).toHaveBeenCalledTimes(4);
    expect(metrics.attemptsAdd).toHaveBeenLastCalledWith(
      1,
      expect.objectContaining({
        attempt: 4,
        result: 'http_error',
        will_retry: false,
        http_status_code: 502,
      }),
    );
    expect(metrics.requestsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        result: 'http_error',
        attempt_count: 4,
        http_status_code: 502,
        error_kind: ErrorKind.PushFailed,
        error_reason: ErrorReason.HTTP,
      }),
    );
  });

  test('records fetch failed retry attempts', async () => {
    vi.useFakeTimers();
    mockFetch
      .mockRejectedValueOnce(new TypeError('fetch failed'))
      .mockResolvedValueOnce(
        new Response(JSON.stringify({success: true}), {status: 200}),
      );

    const promise = fetchWithContext(validator, 'push', {operation: 'mutate'});

    await vi.advanceTimersByTimeAsync(1200);
    await promise;

    expect(metrics.attemptsAdd).toHaveBeenNthCalledWith(
      1,
      1,
      expect.objectContaining({
        attempt: 1,
        result: 'fetch_error',
        will_retry: true,
      }),
    );
    expect(metrics.requestsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        result: 'success',
        attempt_count: 2,
      }),
    );
  });

  test('records parse failures as parse_error', async () => {
    mockFetch.mockResolvedValueOnce(new Response('not-json', {status: 200}));

    await expect(
      fetchWithContext(validator, 'push', {operation: 'mutate'}),
    ).rejects.toThrow(/Failed to parse response/);

    expect(metrics.attemptsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        result: 'parse_error',
        http_status_code: 200,
        error_kind: ErrorKind.PushFailed,
        error_reason: ErrorReason.Parse,
      }),
    );
    expect(metrics.requestsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        result: 'parse_error',
        attempt_count: 1,
        http_status_code: 200,
      }),
    );
  });

  test('records URL allowlist failures without attempts', async () => {
    await expect(
      fetchWithContext(
        validator,
        'push',
        {
          operation: 'mutate',
        },
        {
          url: 'https://evil.example.com/endpoint',
        },
      ),
    ).rejects.toThrow(/not allowed by the ZERO_MUTATE_URL configuration/);

    expect(metrics.attemptsAdd).not.toHaveBeenCalled();
    expect(metrics.requestsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        operation: 'mutate',
        result: 'url_not_allowed',
        attempt_count: 0,
        error_kind: ErrorKind.PushFailed,
        error_reason: ErrorReason.Internal,
      }),
    );
  });

  test('records reserved query params as config_error without attempts', async () => {
    await expect(
      fetchWithContext(
        validator,
        'push',
        {operation: 'mutate'},
        {url: `${baseUrl}?schema=value`},
      ),
    ).rejects.toThrow(/reserved query param "schema"/);

    expect(metrics.attemptsAdd).not.toHaveBeenCalled();
    expect(metrics.requestsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        operation: 'mutate',
        result: 'config_error',
        attempt_count: 0,
      }),
    );
  });

  test('records cleanup type labels', async () => {
    mockFetch
      .mockResolvedValueOnce(new Response(JSON.stringify({success: true})))
      .mockResolvedValueOnce(new Response(JSON.stringify({success: true})));

    await fetchWithContext(validator, 'push', {
      operation: 'cleanup',
      cleanupType: 'single',
    });
    await fetchWithContext(validator, 'push', {
      operation: 'cleanup',
      cleanupType: 'bulk',
    });

    expect(metrics.requestsAdd).toHaveBeenNthCalledWith(
      1,
      1,
      expect.objectContaining({
        operation: 'cleanup',
        cleanup_type: 'single',
        result: 'success',
      }),
    );
    expect(metrics.requestsAdd).toHaveBeenNthCalledWith(
      2,
      1,
      expect.objectContaining({
        operation: 'cleanup',
        cleanup_type: 'bulk',
        result: 'success',
      }),
    );
  });

  test('records validate_auth operation labels', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true}), {status: 200}),
    );

    await fetchWithContext(validator, 'transform', {
      operation: 'validate_auth',
    });

    expect(metrics.requestsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        operation: 'validate_auth',
        result: 'success',
      }),
    );
  });

  test('records top-level API error responses as api_error', async () => {
    const responsePayload = {
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Internal,
      message: 'app returned an error',
      mutationIDs: [],
    } as const;
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(responsePayload), {status: 200}),
    );

    const result = await fetchWithContext(mutateResponseSchema, 'push', {
      operation: 'mutate',
    });

    expect(result).toEqual(responsePayload);
    expect(metrics.attemptsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        result: 'api_error',
        http_status_code: 200,
        error_kind: ErrorKind.PushFailed,
        error_reason: ErrorReason.Internal,
      }),
    );
    expect(metrics.requestsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        result: 'api_error',
        attempt_count: 1,
        error_kind: ErrorKind.PushFailed,
        error_reason: ErrorReason.Internal,
      }),
    );
  });

  test('preserves unknown fields from successful API responses', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true, ignored: 'value'}), {
        status: 200,
      }),
    );

    const result = await fetchWithContext(validator, 'push', {
      operation: 'mutate',
    });

    expect(result).toEqual({success: true, ignored: 'value'});
  });

  test('parses legacy query responses through the helper', async () => {
    const legacyResponse = [
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

    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(legacyResponse), {status: 200}),
    );

    const result = await fetchWithContext(queryResponseSchema, 'transform', {
      operation: 'query',
    });

    expect(result).toEqual(legacyResponse);
  });

  test('preserves existing query params when appending reserved ones', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true}), {status: 200}),
    );
    const urlWithQuery = `${baseUrl}?foo=bar`;

    await fetchWithContext(
      validator,
      'push',
      {operation: 'mutate'},
      {url: urlWithQuery},
    );

    const calledUrl = mockFetch.mock.calls[0]![0] as string;
    const url = new URL(calledUrl);
    expect(url.searchParams.get('foo')).toBe('bar');
    expect(url.searchParams.get('schema')).toBe('test_app_1');
    expect(url.searchParams.get('appID')).toBe('test_app');
  });

  test('omits optional headers when they are not provided', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true}), {status: 200}),
    );

    await fetchWithContext(validator, 'push', {operation: 'mutate'});

    const init = mockFetch.mock.calls[0]![1];
    expect(init?.headers).toEqual({'Content-Type': 'application/json'});
  });

  test('includes forwarded headers in request', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true}), {status: 200}),
    );

    await fetchWithContext(
      validator,
      'push',
      {operation: 'mutate'},
      {
        headerOptions: {
          customHeaders: {
            'x-vercel-automation-bypass-secret': 'my-secret',
            'x-custom-header': 'custom-value',
          },
          requestHeaders: {
            'x-forwarded-for': '203.0.113.1',
          },
        },
      },
    );

    const init = mockFetch.mock.calls[0]![1];
    expect(init?.headers).toEqual({
      'Content-Type': 'application/json',
      'x-vercel-automation-bypass-secret': 'my-secret',
      'x-custom-header': 'custom-value',
      'x-forwarded-for': '203.0.113.1',
    });
  });

  test('customHeaders combined with other headers when allowed', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true}), {status: 200}),
    );

    await fetchWithContext(
      validator,
      'push',
      {operation: 'mutate'},
      {
        headerOptions: {
          apiKey: 'api-key',
          customHeaders: {
            'x-vercel-automation-bypass-secret': 'my-secret',
          },
        },
        auth: 'jwt-token',
      },
    );

    const init = mockFetch.mock.calls[0]![1];
    expect(init?.headers).toEqual({
      'Content-Type': 'application/json',
      'X-Api-Key': 'api-key',
      'x-vercel-automation-bypass-secret': 'my-secret',
      'Authorization': 'Bearer jwt-token',
    });
  });

  test('rejects URLs that are not allowed by configuration for push', async () => {
    await expect(
      fetchFromAPIServer(
        validator,
        'push',
        lc,
        makeContext({url: 'https://evil.example.com/endpoint'}),
        shard,
        body,
        {operation: 'mutate'},
      ),
    ).rejects.toThrow(
      'URL "https://evil.example.com/endpoint" is not allowed by the ZERO_MUTATE_URL configuration',
    );
    expect(mockFetch).not.toHaveBeenCalled();
  });

  test('rejects URLs that are not allowed by configuration for transform', async () => {
    await expect(
      fetchFromAPIServer(
        validator,
        'transform',
        lc,
        makeContext({url: 'https://evil.example.com/endpoint'}),
        shard,
        body,
        {operation: 'query'},
      ),
    ).rejects.toThrow(
      'URL "https://evil.example.com/endpoint" is not allowed by the ZERO_QUERY_URL configuration',
    );
    expect(mockFetch).not.toHaveBeenCalled();
  });

  test.each(['schema', 'appID'] as const)(
    'throws when reserved query param %s is present',
    async reserved => {
      const url = `${baseUrl}?${reserved}=value`;
      await expect(
        fetchFromAPIServer(
          validator,
          'push',
          lc,
          makeContext({url}),
          shard,
          body,
          {operation: 'mutate'},
        ),
      ).rejects.toThrow(
        `The push URL cannot contain the reserved query param "${reserved}"`,
      );
    },
  );

  test('wraps non-OK responses in ProtocolError with http type', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response('failure-body', {status: 503}),
    );

    let caught: unknown;
    try {
      await fetchWithContext(validator, 'push', {operation: 'mutate'});
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ProtocolError);
    assert(isProtocolError(caught), 'Expected protocol error');
    expect(caught.kind).toBe(ErrorKind.PushFailed);
    assert(
      caught.errorBody.kind === ErrorKind.PushFailed,
      'Expected zeroCache PushFailed error',
    );

    expect(caught.errorBody.reason).toBe(ErrorReason.HTTP);
    assert(
      caught.errorBody.reason === ErrorReason.HTTP,
      'Expected zeroCache HTTP error',
    );
    expect(caught.errorBody.status).toBe(503);
    expect(caught.errorBody.bodyPreview).toBe('failure-body');
    expect(caught.errorBody.message).toMatch(/non-OK status 503/);
  });

  test('wraps JSON parse failures in ProtocolError with parse type', async () => {
    const response = new Response('not-json', {status: 200});
    Object.defineProperty(response, 'json', {
      value: vi.fn().mockRejectedValue(new Error('bad json')),
    });
    mockFetch.mockResolvedValueOnce(response);

    let caught: unknown;
    try {
      await fetchWithContext(validator, 'push', {operation: 'mutate'});
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ProtocolError);

    assert(isProtocolError(caught), 'Expected protocol error');
    expect(caught.kind).toBe(ErrorKind.PushFailed);
    assert(
      caught.errorBody.kind === ErrorKind.PushFailed,
      'Expected zeroCache PushFailed error',
    );
    expect(caught.errorBody.reason).toBe(ErrorReason.Parse);
    expect(caught.errorBody.message).toMatch(/Failed to parse response/);
  });

  test('wraps JSON parse failures for transform in ProtocolError with parse type', async () => {
    const response = new Response('not-json', {status: 200});
    Object.defineProperty(response, 'json', {
      value: vi.fn().mockRejectedValue(new Error('bad json')),
    });
    mockFetch.mockResolvedValueOnce(response);

    let caught: unknown;
    try {
      await fetchWithContext(validator, 'transform', {operation: 'query'});
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ProtocolError);

    assert(isProtocolError(caught), 'Expected protocol error');
    expect(caught.kind).toBe(ErrorKind.TransformFailed);
    assert(
      caught.errorBody.kind === ErrorKind.TransformFailed,
      'Expected zeroCache TransformFailed error',
    );
    expect(caught.errorBody.reason).toBe(ErrorReason.Parse);
    expect(caught.errorBody.message).toMatch(/Failed to parse response/);
  });

  test('fails with transform failed when transform is passed', async () => {
    const response = new Response('not-json', {status: 400});
    mockFetch.mockResolvedValueOnce(response);

    let caught: unknown;
    try {
      await fetchWithContext(validator, 'transform', {operation: 'query'});
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ProtocolError);

    assert(isProtocolError(caught), 'Expected protocol error');
    expect(caught.kind).toBe(ErrorKind.TransformFailed);
    assert(
      caught.errorBody.kind === ErrorKind.TransformFailed,
      'Expected zeroCache TransformFailed error',
    );
    expect(caught.errorBody.reason).toBe(ErrorReason.HTTP);
    expect(caught.errorBody.message).toMatch(
      /Fetch from API server returned non-OK status 400/,
    );
  });

  test('wraps validator parse failures in ProtocolError with parse type', async () => {
    const strictValidator = v.object({count: v.number()});
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({count: 'not-a-number'}), {status: 200}),
    );

    let caught: unknown;
    try {
      await fetchWithContext(strictValidator, 'push', {operation: 'mutate'});
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ProtocolError);
    assert(isProtocolError(caught), 'Expected protocol error');

    expect(caught.kind).toBe(ErrorKind.PushFailed);
    assert(
      caught.errorBody.kind === ErrorKind.PushFailed,
      'Expected zeroCache PushFailed error',
    );
    expect(caught.errorBody.reason).toBe(ErrorReason.Parse);
    expect(caught.errorBody.message).toMatch(/Failed to parse response/);
  });

  test('wraps validator parse failures for transform in ProtocolError with parse type', async () => {
    const strictValidator = v.object({count: v.number()});
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({count: 'not-a-number'}), {status: 200}),
    );

    let caught: unknown;
    try {
      await fetchWithContext(strictValidator, 'transform', {
        operation: 'query',
      });
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ProtocolError);
    assert(isProtocolError(caught), 'Expected protocol error');

    expect(caught.kind).toBe(ErrorKind.TransformFailed);
    assert(
      caught.errorBody.kind === ErrorKind.TransformFailed,
      'Expected zeroCache TransformFailed error',
    );
    expect(caught.errorBody.reason).toBe(ErrorReason.Parse);
    expect(caught.errorBody.message).toMatch(/Failed to parse response/);
  });

  test('wraps rejected fetch calls for push in ProtocolError with internal type', async () => {
    mockFetch.mockRejectedValueOnce(new Error('boom'));

    let caught: unknown;
    try {
      await fetchWithContext(validator, 'push', {operation: 'mutate'});
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ProtocolError);
    assert(isProtocolError(caught), 'Expected protocol error');

    expect(caught.kind).toBe(ErrorKind.PushFailed);
    assert(
      caught.errorBody.kind === ErrorKind.PushFailed,
      'Expected zeroCache PushFailed error',
    );
    expect(caught.errorBody.reason).toBe(ErrorReason.Internal);
    expect(caught.errorBody.message).toMatch(/threw error: boom/);
  });

  test('wraps rejected fetch calls for transform in ProtocolError with internal type', async () => {
    mockFetch.mockRejectedValueOnce(new Error('network failure'));

    let caught: unknown;
    try {
      await fetchWithContext(validator, 'transform', {operation: 'query'});
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ProtocolError);
    assert(isProtocolError(caught), 'Expected protocol error');

    expect(caught.kind).toBe(ErrorKind.TransformFailed);
    assert(
      caught.errorBody.kind === ErrorKind.TransformFailed,
      'Expected zeroCache TransformFailed error',
    );
    expect(caught.errorBody.reason).toBe(ErrorReason.Internal);
    expect(caught.errorBody.message).toMatch(/threw error: network failure/);
  });

  describe('retries', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    test('retries on 502 and succeeds', async () => {
      mockFetch
        .mockResolvedValueOnce(new Response('bad gateway', {status: 502}))
        .mockResolvedValueOnce(new Response('bad gateway', {status: 502}))
        .mockResolvedValueOnce(
          new Response(JSON.stringify({success: true}), {status: 200}),
        );

      const promise = fetchWithContext(validator, 'push', {
        operation: 'mutate',
      });

      // 1st retry
      await vi.advanceTimersByTimeAsync(1200);
      // 2nd retry
      await vi.advanceTimersByTimeAsync(1200);

      const result = await promise;
      expect(result).toEqual({success: true});
      expect(mockFetch).toHaveBeenCalledTimes(3);
    });

    test('retries on 504 and succeeds', async () => {
      mockFetch
        .mockResolvedValueOnce(new Response('gateway timeout', {status: 504}))
        .mockResolvedValueOnce(
          new Response(JSON.stringify({success: true}), {status: 200}),
        );

      const promise = fetchWithContext(validator, 'push', {
        operation: 'mutate',
      });

      await vi.advanceTimersByTimeAsync(1200);

      const result = await promise;
      expect(result).toEqual({success: true});
      expect(mockFetch).toHaveBeenCalledTimes(2);
    });

    test('retries on fetch failed and succeeds', async () => {
      mockFetch
        .mockRejectedValueOnce(new TypeError('fetch failed'))
        .mockResolvedValueOnce(
          new Response(JSON.stringify({success: true}), {status: 200}),
        );

      const promise = fetchWithContext(validator, 'push', {
        operation: 'mutate',
      });

      await vi.advanceTimersByTimeAsync(1200);

      const result = await promise;
      expect(result).toEqual({success: true});
      expect(mockFetch).toHaveBeenCalledTimes(2);
    });

    test('fails after max retries (status code)', async () => {
      mockFetch.mockResolvedValue(new Response('bad gateway', {status: 502}));

      const promise = fetchWithContext(validator, 'push', {
        operation: 'mutate',
      });

      // Exhaust all retries
      await Promise.all([
        expect(promise).rejects.toThrow(/non-OK status 502/),
        vi.advanceTimersByTimeAsync(5000),
      ]);

      // Initial + 3 retries (max attempts 4) = 4 calls
      expect(mockFetch).toHaveBeenCalledTimes(4);
    });

    test('fails after max retries (fetch failed)', async () => {
      mockFetch.mockRejectedValue(new TypeError('fetch failed'));

      const promise = fetchWithContext(validator, 'push', {
        operation: 'mutate',
      });

      // Exhaust all retries
      await Promise.all([
        expect(promise).rejects.toThrow(/threw error: fetch failed/),
        vi.advanceTimersByTimeAsync(5000),
      ]);

      // Initial + 3 retries = 4 calls
      expect(mockFetch).toHaveBeenCalledTimes(4);
    });

    test('does not retry on other errors', async () => {
      mockFetch.mockResolvedValue(new Response('bad request', {status: 400}));

      const promise = fetchWithContext(validator, 'push', {
        operation: 'mutate',
      });

      await expect(promise).rejects.toThrow(/non-OK status 400/);
      expect(mockFetch).toHaveBeenCalledTimes(1);
    });
  });
});

describe('getBodyPreview', () => {
  const lc = createSilentLogContext();

  test('returns entire body when below truncation threshold', async () => {
    const res = new Response('short-body', {status: 200});
    expect(await getBodyPreview(res, lc)).toBe('short-body');
  });

  test('truncates body to 512 characters and appends ellipsis', async () => {
    const longBody = 'a'.repeat(600);
    const res = new Response(longBody, {status: 200});
    const preview = await getBodyPreview(res, lc);
    expect(preview).toHaveLength(515);
    expect(preview?.endsWith('...')).toBe(true);
    expect(preview?.startsWith('a'.repeat(512))).toBe(true);
  });

  test('logs warning and returns undefined when preview extraction fails', async () => {
    const sink = new TestLogSink();
    const logContext = new LogContext('debug', undefined, sink);
    const failingResponse = {
      url: 'https://api.example.com/resource',
      clone: () => ({
        text: () => Promise.reject(new Error('read failed')),
      }),
    } as unknown as Response;

    expect(await getBodyPreview(failingResponse, logContext)).toBeUndefined();
    expect(sink.messages).toHaveLength(1);
    const [level, _ctx, args] = sink.messages[0]!;
    expect(level).toBe('warn');
    expect(args[0]).toBe('failed to get body preview');
  });
});

describe('compileUrlPattern', () => {
  test('compiles valid patterns and matches expected URLs', () => {
    const pattern = compileUrlPattern('https://*.example.com/api/*');
    expect(pattern.test('https://api.example.com/api/v1')).toBe(true);
    expect(pattern.test('https://foo.bar.example.com/api/v2')).toBe(true);
    expect(pattern.test('https://example.org/api/v1')).toBe(false);
  });

  test('throws when the pattern is invalid', () => {
    expect(() => compileUrlPattern(':::invalid')).toThrow(
      /Invalid URLPattern in URL configuration/,
    );
  });
});

describe('urlMatch', () => {
  test('returns true when a pattern matches the URL', () => {
    expect(
      urlMatch(
        'https://api.example.com/endpoint?foo=bar',
        ['https://api.example.com/endpoint'].map(compileUrlPattern),
      ),
    ).toBe(true);
  });

  test('returns false when no patterns match the URL', () => {
    expect(
      urlMatch(
        'https://api.example.com/other',
        ['https://api.example.com/endpoint'].map(compileUrlPattern),
      ),
    ).toBe(false);
  });
});
