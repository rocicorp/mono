import {afterEach, expect, jest, test} from '@jest/globals';
import {DNSRecords} from './dns-records.js';
import {mockFetch} from './fetch-test-helper.js';

afterEach(() => {
  jest.restoreAllMocks();
});

test('dns-records', async () => {
  const fetch = mockFetch().default({});

  const resource = new DNSRecords({apiToken: 'api-token', zoneID: 'zone-id'});
  await resource.list();
  await resource.delete('dns-record-id');

  expect(fetch.requests()).toEqual([
    ['GET', 'https://api.cloudflare.com/client/v4/zones/zone-id/dns_records'],
    [
      'DELETE',
      'https://api.cloudflare.com/client/v4/zones/zone-id/dns_records/dns-record-id',
    ],
  ]);
});
