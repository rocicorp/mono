import {afterEach, expect, jest, test} from '@jest/globals';
import {CustomHostnames} from './custom-hostnames.js';

afterEach(() => {
  jest.restoreAllMocks();
});

test('custom-hostnames', async () => {
  const fetchSpy = jest.spyOn(globalThis, 'fetch').mockImplementation(() =>
    Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({success: true}),
    } as unknown as Response),
  );

  const resource = new CustomHostnames('api-token', 'zone-id');

  await resource.list();

  expect(fetchSpy).toHaveBeenCalledTimes(1);
  expect(fetchSpy.mock.calls[0][0]).toEqual(
    'https://api.cloudflare.com/client/v4/zones/zone-id/custom_hostnames',
  );

  expect(fetchSpy.mock.calls[0][1]?.method).toEqual('GET');

  await resource.delete('hostname-id');

  expect(fetchSpy).toHaveBeenCalledTimes(2);
  expect(fetchSpy.mock.calls[1][0]).toEqual(
    'https://api.cloudflare.com/client/v4/zones/zone-id/custom_hostnames/hostname-id',
  );

  expect(fetchSpy.mock.calls[1][1]?.method).toEqual('DELETE');
});
