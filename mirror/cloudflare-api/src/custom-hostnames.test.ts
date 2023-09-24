import {afterEach, expect, jest, test} from '@jest/globals';
import {CustomHostnames} from './custom-hostnames.js';
import {mockFetch} from './fetch-test-helper.js';

afterEach(() => {
  jest.restoreAllMocks();
});

test('custom-hostnames', async () => {
  const fetch = mockFetch().default({});

  const resource = new CustomHostnames('api-token', 'zone-id');
  await resource.list();
  await resource.delete('hostname-id');

  expect(fetch.requests()).toEqual([
    [
      'GET',
      'https://api.cloudflare.com/client/v4/zones/zone-id/custom_hostnames',
    ],
    [
      'DELETE',
      'https://api.cloudflare.com/client/v4/zones/zone-id/custom_hostnames/hostname-id',
    ],
  ]);
});
