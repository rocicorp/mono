import {afterEach, expect, jest, test} from '@jest/globals';
import {DNSRecords} from './dns-records.js';

afterEach(() => {
  jest.restoreAllMocks();
});

test('dns-records', async () => {
  const fetchSpy = jest.spyOn(globalThis, 'fetch').mockImplementation(() =>
    Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({success: true}),
    } as unknown as Response),
  );

  const resource = new DNSRecords('api-token', 'zone-id');

  await resource.list();

  expect(fetchSpy).toHaveBeenCalledTimes(1);
  expect(fetchSpy.mock.calls[0][0]).toEqual(
    'https://api.cloudflare.com/client/v4/zones/zone-id/dns_records',
  );

  expect(fetchSpy.mock.calls[0][1]?.method).toEqual('GET');

  await resource.delete('dns-record-id');

  expect(fetchSpy).toHaveBeenCalledTimes(2);
  expect(fetchSpy.mock.calls[1][0]).toEqual(
    'https://api.cloudflare.com/client/v4/zones/zone-id/dns_records/dns-record-id',
  );

  expect(fetchSpy.mock.calls[1][1]?.method).toEqual('DELETE');
});
