import {
  afterEach,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import {FetchMocker} from 'shared/src/fetch-mocker.js';
import {CustomHostnames} from './custom-hostnames.js';
import {mockFetch} from './fetch-test-helper.js';

describe('cf fetch', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  const resource = new CustomHostnames({
    apiToken: 'api-token',
    zoneID: 'zone-id',
  });

  const expectedRequests = (num: number) =>
    Array(num).fill([
      'GET',
      'https://api.cloudflare.com/client/v4/zones/zone-id/custom_hostnames/ch-id',
    ]);

  describe('fetch errors', () => {
    type Case = {
      name: string;
      code: number;
      message: string;
      expectedStack: string;
    };
    const cases: Case[] = [
      {
        name: 'reference error',
        code: 10021,
        message:
          `Uncaught ReferenceError: window is not defined\n` +
          `  at index.js:169:23\n` +
          `  at index.js:41:70\n` +
          `  at index.js:42:7 in node_modules/leaflet/dist/leaflet-src.js\n` +
          `  at index.js:11:50 in __require\n` +
          `  at index.js:9701:30\n`,
        expectedStack: `  at Uncaught ReferenceError: window is not defined (cloudflare:10021)\n`,
      },
      {
        name: 'cloudflare outage',
        code: 10000,
        message: 'Internal authentication error: internal server error',
        expectedStack: `  at Internal authentication error: internal server error (cloudflare:10000)\n`,
      },
    ];
    for (const c of cases) {
      test(c.name, async () => {
        mockFetch().error('GET', 'custom_hostnames', c.code, c.message);

        const result = await resource.get('ch-id').catch(err => err);
        expect(result).toBeInstanceOf(Error);
        expect(
          (result as Error).stack?.indexOf(c.expectedStack),
        ).toBeGreaterThan(0);
      });
    }
  });

  test('exponential backoff with recovery', async () => {
    const fetch = new FetchMocker()
      .error('GET', 'custom_hostnames', 504, 'Gateway Timeout')
      .once()
      .error('GET', 'custom_hostnames', 504, 'Gateway Timeout')
      .once()
      .result('GET', 'custom_hostnames', {success: true, result: {foo: 'bar'}});

    const result = resource.get('ch-id');

    expect(fetch.requests()).toEqual(expectedRequests(1));

    await jest.advanceTimersByTimeAsync(2000);
    expect(fetch.requests()).toEqual(expectedRequests(2));

    await jest.advanceTimersByTimeAsync(3000);
    expect(fetch.requests()).toEqual(expectedRequests(3));

    expect(await result).toEqual({foo: 'bar'});
  });

  test('exponential backoff with final error', async () => {
    const fetch = new FetchMocker().default(504, 'Gateway Timeout');

    const result = resource.get('ch-id').catch(e => e);

    expect(fetch.requests()).toEqual(expectedRequests(1));

    await jest.advanceTimersByTimeAsync(2000);
    expect(fetch.requests()).toEqual(expectedRequests(2));

    await jest.advanceTimersByTimeAsync(3000);
    expect(fetch.requests()).toEqual(expectedRequests(3));

    await jest.advanceTimersByTimeAsync(4500);
    expect(fetch.requests()).toEqual(expectedRequests(4));

    await jest.advanceTimersByTimeAsync(6750);
    expect(fetch.requests()).toEqual(expectedRequests(5));

    await jest.advanceTimersByTimeAsync(10125);
    expect(fetch.requests()).toEqual(expectedRequests(6));

    await jest.advanceTimersByTimeAsync(15188);
    expect(fetch.requests()).toEqual(expectedRequests(7));

    expect(await result).toBeInstanceOf(Error);
  });

  test('no exponential backoff for 4xx responses', async () => {
    const fetch = new FetchMocker().error(
      'GET',
      'custom_hostnames',
      400,
      'Bad Request',
    );

    const result = await resource.get('ch-id').catch(e => e);
    expect(result).toBeInstanceOf(Error);

    expect(fetch.requests()).toEqual(expectedRequests(1));
  });
});
