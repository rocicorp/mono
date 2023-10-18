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
