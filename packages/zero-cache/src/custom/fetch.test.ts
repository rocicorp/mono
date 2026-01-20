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
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import {
  ProtocolError,
  isProtocolError,
} from '../../../zero-protocol/src/error.ts';
import type {ShardID} from '../types/shards.ts';
import {
  compileUrlPattern,
  fetchFromAPIServer,
  getBodyPreview,
  urlMatch,
} from './fetch.ts';

const mockFetch = vi.fn() as MockedFunction<typeof fetch>;
vi.stubGlobal('fetch', mockFetch);

const shard: ShardID = {appID: 'test_app', shardNum: 1};
const baseUrl = 'https://api.example.com/endpoint';
const allowedPatterns = [compileUrlPattern(baseUrl)];

afterAll(() => {
  vi.unstubAllGlobals();
});

describe('fetchFromAPIServer', () => {
  const lc = createSilentLogContext();
  const body = {test: 'data'};
  const validator = v.object({success: v.boolean()});

  beforeEach(() => {
    mockFetch.mockReset();
    vi.useRealTimers();
  });

  test('returns parsed JSON on success and sends expected headers', async () => {
    const responsePayload = {success: true};
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(responsePayload), {status: 200}),
    );

    const result = await fetchFromAPIServer(
      validator,
      'push',
      lc,
      baseUrl,
      false,
      allowedPatterns,
      shard,
      {
        apiKey: 'key-123',
        token: 'token-abc',
        cookie: 'session=xyz',
      },
      body,
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

  test('preserves existing query params when appending reserved ones', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true}), {status: 200}),
    );
    const urlWithQuery = `${baseUrl}?foo=bar`;

    await fetchFromAPIServer(
      validator,
      'push',
      lc,
      urlWithQuery,
      true,
      allowedPatterns,
      shard,
      {},
      body,
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

    await fetchFromAPIServer(
      validator,
      'push',
      lc,
      baseUrl,
      true,
      allowedPatterns,
      shard,
      {},
      body,
    );

    const init = mockFetch.mock.calls[0]![1];
    expect(init?.headers).toEqual({'Content-Type': 'application/json'});
  });

  test('includes customHeaders in request when allowed', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true}), {status: 200}),
    );

    await fetchFromAPIServer(
      validator,
      'push',
      lc,
      baseUrl,
      true,
      allowedPatterns,
      shard,
      {
        customHeaders: {
          'x-vercel-automation-bypass-secret': 'my-secret',
          'x-custom-header': 'custom-value',
        },
        allowedClientHeaders: [
          'x-vercel-automation-bypass-secret',
          'x-custom-header',
        ],
      },
      body,
    );

    const init = mockFetch.mock.calls[0]![1];
    expect(init?.headers).toEqual({
      'Content-Type': 'application/json',
      'x-vercel-automation-bypass-secret': 'my-secret',
      'x-custom-header': 'custom-value',
    });
  });

  test('customHeaders combined with other headers when allowed', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true}), {status: 200}),
    );

    await fetchFromAPIServer(
      validator,
      'push',
      lc,
      baseUrl,
      true,
      allowedPatterns,
      shard,
      {
        apiKey: 'api-key',
        customHeaders: {
          'x-vercel-automation-bypass-secret': 'my-secret',
        },
        allowedClientHeaders: ['x-vercel-automation-bypass-secret'],
        token: 'jwt-token',
      },
      body,
    );

    const init = mockFetch.mock.calls[0]![1];
    expect(init?.headers).toEqual({
      'Content-Type': 'application/json',
      'X-Api-Key': 'api-key',
      'x-vercel-automation-bypass-secret': 'my-secret',
      'Authorization': 'Bearer jwt-token',
    });
  });

  test('filters out all client headers when allowedClientHeaders is not set', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true}), {status: 200}),
    );

    await fetchFromAPIServer(
      validator,
      'push',
      lc,
      baseUrl,
      true,
      allowedPatterns,
      shard,
      {
        customHeaders: {
          'x-request-id': '123',
          'x-custom': 'value',
        },
        // allowedClientHeaders not set
      },
      body,
    );

    const init = mockFetch.mock.calls[0]![1];
    expect(init?.headers).toEqual({
      'Content-Type': 'application/json',
    });
  });

  test('filters out all client headers when allowedClientHeaders is empty', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true}), {status: 200}),
    );

    await fetchFromAPIServer(
      validator,
      'push',
      lc,
      baseUrl,
      true,
      allowedPatterns,
      shard,
      {
        customHeaders: {
          'x-request-id': '123',
          'x-custom': 'value',
        },
        allowedClientHeaders: [],
      },
      body,
    );

    const init = mockFetch.mock.calls[0]![1];
    expect(init?.headers).toEqual({
      'Content-Type': 'application/json',
    });
  });

  test('only forwards headers in allowedClientHeaders list', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true}), {status: 200}),
    );

    await fetchFromAPIServer(
      validator,
      'push',
      lc,
      baseUrl,
      true,
      allowedPatterns,
      shard,
      {
        customHeaders: {
          'x-request-id': '123',
          'x-forbidden': 'bad',
          'X-Correlation-ID': 'abc',
        },
        allowedClientHeaders: ['x-request-id', 'x-correlation-id'],
      },
      body,
    );

    const init = mockFetch.mock.calls[0]![1];
    expect(init?.headers).toEqual({
      'Content-Type': 'application/json',
      'x-request-id': '123',
      'X-Correlation-ID': 'abc',
    });
  });

  test('header filtering is case-insensitive', async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({success: true}), {status: 200}),
    );

    await fetchFromAPIServer(
      validator,
      'push',
      lc,
      baseUrl,
      true,
      allowedPatterns,
      shard,
      {
        customHeaders: {
          'X-REQUEST-ID': 'uppercase',
          'x-custom-header': 'lowercase',
        },
        allowedClientHeaders: ['x-request-id', 'X-CUSTOM-HEADER'],
      },
      body,
    );

    const init = mockFetch.mock.calls[0]![1];
    expect(init?.headers).toEqual({
      'Content-Type': 'application/json',
      'X-REQUEST-ID': 'uppercase',
      'x-custom-header': 'lowercase',
    });
  });

  test('rejects URLs that are not allowed by configuration for push', async () => {
    await expect(
      fetchFromAPIServer(
        validator,
        'push',
        lc,
        'https://evil.example.com/endpoint',
        true,
        allowedPatterns,
        shard,
        {},
        body,
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
        'https://evil.example.com/endpoint',
        true,
        allowedPatterns,
        shard,
        {},
        body,
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
          url,
          false,
          allowedPatterns,
          shard,
          {},
          body,
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
      await fetchFromAPIServer(
        validator,
        'push',
        lc,
        baseUrl,
        false,
        allowedPatterns,
        shard,
        {},
        body,
      );
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
      await fetchFromAPIServer(
        validator,
        'push',
        lc,
        baseUrl,
        false,
        allowedPatterns,
        shard,
        {},
        body,
      );
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
      await fetchFromAPIServer(
        validator,
        'transform',
        lc,
        baseUrl,
        false,
        allowedPatterns,
        shard,
        {},
        body,
      );
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
      await fetchFromAPIServer(
        validator,
        'transform',
        lc,
        baseUrl,
        false,
        allowedPatterns,
        shard,
        {},
        body,
      );
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
      await fetchFromAPIServer(
        strictValidator,
        'push',
        lc,
        baseUrl,
        false,
        allowedPatterns,
        shard,
        {},
        body,
      );
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
      await fetchFromAPIServer(
        strictValidator,
        'transform',
        lc,
        baseUrl,
        false,
        allowedPatterns,
        shard,
        {},
        body,
      );
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
      await fetchFromAPIServer(
        validator,
        'push',
        lc,
        baseUrl,
        false,
        allowedPatterns,
        shard,
        {},
        body,
      );
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
    expect(caught.errorBody.message).toMatch(/unknown error: boom/);
  });

  test('wraps rejected fetch calls for transform in ProtocolError with internal type', async () => {
    mockFetch.mockRejectedValueOnce(new Error('network failure'));

    let caught: unknown;
    try {
      await fetchFromAPIServer(
        validator,
        'transform',
        lc,
        baseUrl,
        false,
        allowedPatterns,
        shard,
        {},
        body,
      );
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
    expect(caught.errorBody.message).toMatch(/unknown error: network failure/);
  });
});

describe('getBodyPreview', () => {
  const lc = createSilentLogContext();

  test('returns entire body when below truncation threshold', async () => {
    const res = new Response('short-body', {status: 200});
    expect(await getBodyPreview(res, lc, 'warn')).toBe('short-body');
  });

  test('truncates body to 512 characters and appends ellipsis', async () => {
    const longBody = 'a'.repeat(600);
    const res = new Response(longBody, {status: 200});
    const preview = await getBodyPreview(res, lc, 'warn');
    expect(preview).toHaveLength(515);
    expect(preview?.endsWith('...')).toBe(true);
    expect(preview?.startsWith('a'.repeat(512))).toBe(true);
  });

  test('logs error and returns undefined when preview extraction fails', async () => {
    const sink = new TestLogSink();
    const logContext = new LogContext('debug', undefined, sink);
    const failingResponse = {
      url: 'https://api.example.com/resource',
      clone: () => ({
        text: () => Promise.reject(new Error('read failed')),
      }),
    } as unknown as Response;

    expect(
      await getBodyPreview(failingResponse, logContext, 'error'),
    ).toBeUndefined();
    expect(sink.messages).toHaveLength(1);
    const [level, _ctx, args] = sink.messages[0]!;
    expect(level).toBe('error');
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
