import {
  describe,
  expect,
  vi,
  beforeEach,
  type MockedFunction,
  test,
} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {fetchFromAPIServer, urlMatch} from './fetch.ts';
import {ErrorForClient} from '../types/error-for-client.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import type {ShardID} from '../types/shards.ts';

// Mock the global fetch function
const mockFetch = vi.fn() as MockedFunction<typeof fetch>;
vi.stubGlobal('fetch', mockFetch);

describe('fetchFromAPIServer', () => {
  const mockShard: ShardID = {
    appID: 'test_app',
    shardNum: 1,
  };
  const lc = createSilentLogContext();

  const baseUrl = 'https://api.example.com/endpoint';
  const headerOptions = {
    apiKey: 'test-api-key',
    token: 'test-token',
  };
  const body = {test: 'data'};

  beforeEach(() => {
    mockFetch.mockReset();
  });

  test('should make a POST request with correct headers and body', async () => {
    const mockResponse = new Response(JSON.stringify({success: true}), {
      status: 200,
    });
    mockFetch.mockResolvedValue(mockResponse);

    await fetchFromAPIServer(
      lc,
      baseUrl,
      [baseUrl],
      mockShard,
      headerOptions,
      body,
    );

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('https://api.example.com/endpoint'),
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Api-Key': 'test-api-key',
          // eslint-disable-next-line @typescript-eslint/naming-convention
          'Authorization': 'Bearer test-token',
        },
        body: JSON.stringify(body),
      },
    );
  });

  test('should include API key header when provided', async () => {
    const mockResponse = new Response('{}', {status: 200});
    mockFetch.mockResolvedValue(mockResponse);

    await fetchFromAPIServer(
      lc,
      baseUrl,
      [baseUrl],
      mockShard,
      {apiKey: 'my-key'},
      body,
    );

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.objectContaining({
          'X-Api-Key': 'my-key',
        }),
      }),
    );
  });

  test('should include Authorization header when token is provided', async () => {
    const mockResponse = new Response('{}', {status: 200});
    mockFetch.mockResolvedValue(mockResponse);

    await fetchFromAPIServer(
      lc,
      baseUrl,
      [baseUrl],
      mockShard,
      {token: 'my-token'},
      body,
    );

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.objectContaining({
          // eslint-disable-next-line @typescript-eslint/naming-convention
          Authorization: 'Bearer my-token',
        }),
      }),
    );
  });

  test('should not include auth headers when not provided', async () => {
    const mockResponse = new Response('{}', {status: 200});
    mockFetch.mockResolvedValue(mockResponse);

    await fetchFromAPIServer(
      lc,
      baseUrl,
      [baseUrl],
      mockShard,
      {},
      body,
    );

    const callArgs = mockFetch.mock.calls[0];
    const headers = callArgs[1]?.headers as Record<string, string>;

    expect(headers).not.toHaveProperty('X-Api-Key');
    expect(headers).not.toHaveProperty('Authorization');
    expect(headers).toHaveProperty('Content-Type', 'application/json');
  });

  test('should append required schema and appID parameters', async () => {
    const mockResponse = new Response('{}', {status: 200});
    mockFetch.mockResolvedValue(mockResponse);

    await fetchFromAPIServer(
      lc,
      baseUrl,
      [baseUrl],
      mockShard,
      {},
      body,
    );

    const calledUrl = mockFetch.mock.calls[0][0] as string;
    const url = new URL(calledUrl);

    expect(url.searchParams.get('schema')).toBe('test_app_1');
    expect(url.searchParams.get('appID')).toBe('test_app');
  });

  test('should throw an error if URL contains reserved parameter "schema"', async () => {
    const urlWithReserved = 'https://api.example.com/endpoint?schema=reserved';

    await expect(
      fetchFromAPIServer(
        lc,
        urlWithReserved,
        [baseUrl],
        mockShard,
        {},
        body,
      ),
    ).rejects.toThrow(
      'The push URL cannot contain the reserved query param "schema"',
    );
  });

  test('should throw an error if URL contains reserved parameter "appID"', async () => {
    const urlWithReserved = 'https://api.example.com/endpoint?appID=reserved';

    await expect(
      fetchFromAPIServer(
        lc,
        urlWithReserved,
        [baseUrl],
        mockShard,
        {},
        body,
      ),
    ).rejects.toThrow(
      'The push URL cannot contain the reserved query param "appID"',
    );
  });

  test('should return response for successful requests', async () => {
    const mockResponse = new Response(JSON.stringify({success: true}), {
      status: 200,
    });
    mockFetch.mockResolvedValue(mockResponse);

    const result = await fetchFromAPIServer(
      lc,
      baseUrl,
      [baseUrl],
      mockShard,
      {},
      body,
    );

    expect(result).toBe(mockResponse);
  });

  test('should throw ErrorForClient on 401 unauthorized response', async () => {
    const errorMessage = 'Unauthorized access';

    // First call - just test that it throws ErrorForClient
    const mockResponse1 = new Response(errorMessage, {status: 401});
    mockFetch.mockResolvedValueOnce(mockResponse1);

    await expect(
      fetchFromAPIServer(lc, baseUrl, [baseUrl], mockShard, {}, body),
    ).rejects.toThrow(ErrorForClient);

    // Second call - test the error details
    const mockResponse2 = new Response(errorMessage, {status: 401});
    mockFetch.mockResolvedValueOnce(mockResponse2);

    try {
      await fetchFromAPIServer(
        lc,
        baseUrl,
        [baseUrl],
        mockShard,
        {},
        body,
      );
    } catch (error) {
      expect(error).toBeInstanceOf(ErrorForClient);
      const errorForClient = error as ErrorForClient;
      expect(errorForClient.errorBody.kind).toBe(ErrorKind.AuthInvalidated);
      expect(errorForClient.errorBody.message).toBe(errorMessage);
    }
  });

  test('should not throw for non-401 error status codes', async () => {
    const mockResponse = new Response('Server Error', {status: 500});
    mockFetch.mockResolvedValue(mockResponse);

    const result = await fetchFromAPIServer(
      lc,
      baseUrl,
      [baseUrl],
      mockShard,
      {},
      body,
    );

    expect(result).toBe(mockResponse);
  });

  test('should stringify body as JSON', async () => {
    const mockResponse = new Response('{}', {status: 200});
    mockFetch.mockResolvedValue(mockResponse);
    const complexBody = {
      nested: {
        object: true,
        array: [1, 2, 3],
      },
    };

    await fetchFromAPIServer(
      lc,
      baseUrl,
      [baseUrl],
      mockShard,
      {},
      complexBody,
    );

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        body: JSON.stringify(complexBody),
      }),
    );
  });
});

describe('urlMatch', () => {
  test('should return true for matching URLs', () => {
    expect(
      urlMatch('https://api.example.com/endpoint', [
        'https://api.example.com/endpoint',
      ]),
    ).toBe(true);

    expect(
      urlMatch('https://api.example.com/endpoint', [
        'https://*.example.com/endpoint',
      ]),
    ).toBe(true);

    expect(
      urlMatch('https://api.v1.example.com/endpoint', [
        'https://*.*.example.com/endpoint',
      ]),
    ).toBe(true);

    expect(
      urlMatch('https://api.example.com/endpoint?existing=param', [
        'https://api.example.com/endpoint',
      ]),
    ).toBe(true);

    expect(
      urlMatch('https://api.example.com/endpoint?existing=param', [
        'https://api.example.com/endpoint?other=param',
      ]),
    ).toBe(true);
  });

  test('should return false for non-matching URLs', () => {
    expect(
      urlMatch('https://api.example.com/other-endpoint', [
        'https://api.example.com/endpoint',
      ]),
    ).toBe(false);

    expect(
      urlMatch('https://another-domain.com/endpoint', [
        'https://api.example.com/endpoint',
      ]),
    ).toBe(false);

    // Wildcard with no subdomain
    expect(
      urlMatch('https://example.com/endpoint', [
        'https://*.example.com/endpoint',
      ]),
    ).toBe(false);

    // Wildcard with trailing path
    expect(
      urlMatch('https://api.example.com/endpoint/path', [
        'https://*.example.com/endpoint',
      ]),
    ).toBe(false);

    // Wrong number of subdomains for wildcard
    expect(
      urlMatch('https://api.example.com/endpoint', [
        'https://*.*.example.com/endpoint',
      ]),
    ).toBe(false);

    // wildcards can only match subdomains, not paths or anything else
    expect(
      urlMatch('https://api.example.com/endpoint', [
        'https://*example.com/endpoint',
      ]),
    ).toBe(false);
    expect(
      urlMatch('https://example.com/endpoint', [
        'https://*example.com/endpoint',
      ]),
    ).toBe(false);
    expect(
      urlMatch('https://apiexample.com/endpoint', [
        'https://*example.com/endpoint',
      ]),
    ).toBe(false);
    expect(() =>
      urlMatch('https://*example.com/endpoint', [
        'https://*example.com/endpoint',
      ]),
    ).toThrow();
    expect(
      urlMatch('https://api.example.com/endpoint', [
        'https://api.example.com/*',
      ]),
    ).toBe(false);
  });

  test('should handle empty allowed URLs array', () => {
    expect(urlMatch('https://api.example.com/endpoint', [])).toBe(false);
  });
});
