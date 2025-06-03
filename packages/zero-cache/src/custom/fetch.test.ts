import {
  describe,
  expect,
  vi,
  beforeEach,
  type MockedFunction,
  test,
} from 'vitest';
import {fetchFromAPIServer} from './fetch.ts';
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

  const baseUrl = 'https://api.example.com/endpoint';
  const headerOptions = {
    apiKey: 'test-api-key',
    token: 'test-token',
  };
  const queryParams = {
    param1: 'value1',
    param2: 'value2',
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
      baseUrl,
      mockShard,
      headerOptions,
      queryParams,
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
      baseUrl,
      mockShard,
      {apiKey: 'my-key'},
      undefined,
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
      baseUrl,
      mockShard,
      {token: 'my-token'},
      undefined,
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

    await fetchFromAPIServer(baseUrl, mockShard, {}, undefined, body);

    const callArgs = mockFetch.mock.calls[0];
    const headers = callArgs[1]?.headers as Record<string, string>;

    expect(headers).not.toHaveProperty('X-Api-Key');
    expect(headers).not.toHaveProperty('Authorization');
    expect(headers).toHaveProperty('Content-Type', 'application/json');
  });

  test('should append query parameters to URL', async () => {
    const mockResponse = new Response('{}', {status: 200});
    mockFetch.mockResolvedValue(mockResponse);

    await fetchFromAPIServer(baseUrl, mockShard, {}, queryParams, body);

    const calledUrl = mockFetch.mock.calls[0][0] as string;
    const url = new URL(calledUrl);

    expect(url.searchParams.get('param1')).toBe('value1');
    expect(url.searchParams.get('param2')).toBe('value2');
  });

  test('should append required schema and appID parameters', async () => {
    const mockResponse = new Response('{}', {status: 200});
    mockFetch.mockResolvedValue(mockResponse);

    await fetchFromAPIServer(baseUrl, mockShard, {}, undefined, body);

    const calledUrl = mockFetch.mock.calls[0][0] as string;
    const url = new URL(calledUrl);

    expect(url.searchParams.get('schema')).toBe('test_app_1');
    expect(url.searchParams.get('appID')).toBe('test_app');
  });

  test('should handle URLs that already have query parameters', async () => {
    const mockResponse = new Response('{}', {status: 200});
    mockFetch.mockResolvedValue(mockResponse);
    const urlWithParams = 'https://api.example.com/endpoint?existing=param';

    await fetchFromAPIServer(urlWithParams, mockShard, {}, queryParams, body);

    const calledUrl = mockFetch.mock.calls[0][0] as string;
    const url = new URL(calledUrl);

    expect(url.searchParams.get('existing')).toBe('param');
    expect(url.searchParams.get('param1')).toBe('value1');
    expect(url.searchParams.get('schema')).toBe('test_app_1');
  });

  test('should throw an error if URL contains reserved parameter "schema"', async () => {
    const urlWithReserved = 'https://api.example.com/endpoint?schema=reserved';

    await expect(
      fetchFromAPIServer(urlWithReserved, mockShard, {}, undefined, body),
    ).rejects.toThrow(
      'The push URL cannot contain the reserved query param "schema"',
    );
  });

  test('should throw an error if URL contains reserved parameter "appID"', async () => {
    const urlWithReserved = 'https://api.example.com/endpoint?appID=reserved';

    await expect(
      fetchFromAPIServer(urlWithReserved, mockShard, {}, undefined, body),
    ).rejects.toThrow(
      'The push URL cannot contain the reserved query param "appID"',
    );
  });

  test('should throw an error if query params contain reserved parameter "schema"', async () => {
    const reservedQueryParams = {schema: 'reserved'};

    await expect(
      fetchFromAPIServer(baseUrl, mockShard, {}, reservedQueryParams, body),
    ).rejects.toThrow(
      'The push URL cannot contain the reserved query param "schema"',
    );
  });

  test('should throw an error if query params contain reserved parameter "appID"', async () => {
    const reservedQueryParams = {appID: 'reserved'};

    await expect(
      fetchFromAPIServer(baseUrl, mockShard, {}, reservedQueryParams, body),
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
      baseUrl,
      mockShard,
      {},
      undefined,
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
      fetchFromAPIServer(baseUrl, mockShard, {}, undefined, body),
    ).rejects.toThrow(ErrorForClient);

    // Second call - test the error details
    const mockResponse2 = new Response(errorMessage, {status: 401});
    mockFetch.mockResolvedValueOnce(mockResponse2);

    try {
      await fetchFromAPIServer(baseUrl, mockShard, {}, undefined, body);
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
      baseUrl,
      mockShard,
      {},
      undefined,
      body,
    );

    expect(result).toBe(mockResponse);
  });

  test('should handle empty query params', async () => {
    const mockResponse = new Response('{}', {status: 200});
    mockFetch.mockResolvedValue(mockResponse);

    await fetchFromAPIServer(baseUrl, mockShard, {}, undefined, body);

    const calledUrl = mockFetch.mock.calls[0][0] as string;
    const url = new URL(calledUrl);

    expect(url.searchParams.get('schema')).toBe('test_app_1');
    expect(url.searchParams.get('appID')).toBe('test_app');
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

    await fetchFromAPIServer(baseUrl, mockShard, {}, undefined, complexBody);

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        body: JSON.stringify(complexBody),
      }),
    );
  });
});
