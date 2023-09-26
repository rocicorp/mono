import {afterEach, describe, expect, jest, test} from '@jest/globals';
import {publishCustomHostnames} from './publish-custom-hostnames.js';
import {mockFetch} from 'cloudflare-api/src/fetch-test-helper.js';
import type {ZoneConfig} from './config.js';
import {NamespacedScript} from 'cloudflare-api/src/scripts.js';
import type {DNSRecord} from 'cloudflare-api/src/dns-records.js';
import {Errors, FetchResultError} from 'cloudflare-api/src/fetch.js';

afterEach(() => {
  jest.restoreAllMocks();
});

describe('publish-custom-hostnames', () => {
  const ACCOUNT_ID = '92c9f92f0e';
  const NAMESPACE = 'prod';
  const SCRIPT_NAME = 'foo-script';
  const ZONE_CONFIG: ZoneConfig = {
    apiToken: '29d8fh2e9dfhs9f3euifn',
    zoneID: '1ab3d299c',
    zoneName: 'reflect-o-rama.net',
  };

  const script = new NamespacedScript(
    {apiToken: ZONE_CONFIG.apiToken, accountID: ACCOUNT_ID},
    NAMESPACE,
    SCRIPT_NAME,
  );

  async function publish(hostname: string): Promise<string[]> {
    const msgs: string[] = [];
    for await (const msg of publishCustomHostnames(
      ZONE_CONFIG,
      script,
      hostname,
    )) {
      msgs.push(msg);
    }
    return msgs;
  }

  test('hostname already created', async () => {
    const fetch = mockFetch().result<Partial<DNSRecord>[]>(
      'GET',
      '/dns_records', // DNSRecords.list()
      [{name: 'foo.reflect-o-rama.net'}],
    );

    expect(await publish('foo.reflect-o-rama.net')).toEqual([]);

    expect(fetch.requests()).toEqual([
      [
        'GET',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records?tag=script%3Aprod%2Ffoo-script',
      ],
    ]);
  });

  test('creates new hostname', async () => {
    const fetch = mockFetch()
      .result('GET', '/dns_records', []) // DNSRecords.list()
      .result('POST', '/custom_hostnames', {id: 'ch-id', status: 'initial'}) // CustomHostnames.create()
      .result('POST', '/dns_records', {id: 'record-id'}) // DNSRecords.create()
      .result('PATCH', '/custom_hostnames/ch-id', {status: 'active'}); // CustomHostnames.edit()

    expect(await publish('foo.reflect-o-rama.net')).toEqual([
      'Setting up hostname foo.reflect-o-rama.net',
    ]);

    expect(fetch.requests()).toEqual([
      [
        'GET',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records?tag=script%3Aprod%2Ffoo-script',
      ],
      [
        'POST',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/custom_hostnames',
      ],
      [
        'POST',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records',
      ],
      [
        'PATCH',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/custom_hostnames/ch-id',
      ],
    ]);
    const ch = {
      hostname: 'foo.reflect-o-rama.net',
      // eslint-disable-next-line @typescript-eslint/naming-convention
      custom_metadata: {
        script: 'foo-script',
        namespace: 'prod',
      },
      ssl: {
        method: 'http',
        type: 'dv',
      },
    };
    expect(fetch.jsonPayloads()).toEqual([
      null,
      ch,
      {
        name: 'foo.reflect-o-rama.net',
        type: 'CNAME',
        content: 'reflect-o-rama.net',
        proxied: true,
        tags: ['script:prod/foo-script', 'ch:ch-id'],
        comment: 'Managed by Rocicorp (reflect.net)',
      },
      ch,
    ]);
  });

  test('create is resumable', async () => {
    const fetch = mockFetch()
      .result('GET', '/dns_records', []) // DNSRecords.list()
      .once() // Second call will be return a different value.
      .error('POST', '/custom_hostnames', Errors.DuplicateCustomHostnameFound) // CustomHostnames.create()
      .result('GET', '/custom_hostnames', [{id: 'existing-ch-id'}]) // DNSRecords.list()
      .result('PATCH', '/custom_hostnames/existing-ch-id', {
        id: 'existing-ch-id',
        status: 'active',
      }) // CustomHostnames.edit()
      .error('POST', '/dns_records', Errors.RecordWithHostAlreadyExists) // DNSRecords.create()
      .result('GET', '/dns_records', [{id: 'existing-record-id'}]) // DNSRecords.list()
      .result('PUT', '/dns_records/existing-record-id', {
        id: 'existing-record-id',
      }); // DNSRecords.update()

    expect(await publish('foo.reflect-o-rama.net')).toEqual([
      'Setting up hostname foo.reflect-o-rama.net',
    ]);

    expect(fetch.requests()).toEqual([
      [
        'GET',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records?tag=script%3Aprod%2Ffoo-script',
      ],
      [
        'POST',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/custom_hostnames',
      ],
      [
        'GET',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/custom_hostnames?hostname=foo.reflect-o-rama.net',
      ],
      [
        'PATCH',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/custom_hostnames/existing-ch-id',
      ],
      [
        'POST',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records',
      ],
      [
        'GET',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records?type=CNAME&name=foo.reflect-o-rama.net',
      ],
      [
        'PUT',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records/existing-record-id',
      ],
    ]);
    const ch = {
      hostname: 'foo.reflect-o-rama.net',
      // eslint-disable-next-line @typescript-eslint/naming-convention
      custom_metadata: {
        script: 'foo-script',
        namespace: 'prod',
      },
      ssl: {
        method: 'http',
        type: 'dv',
      },
    };
    const record = {
      name: 'foo.reflect-o-rama.net',
      type: 'CNAME',
      content: 'reflect-o-rama.net',
      proxied: true,
      tags: ['script:prod/foo-script', 'ch:existing-ch-id'],
      comment: 'Managed by Rocicorp (reflect.net)',
    };
    expect(fetch.jsonPayloads()).toEqual([
      null, // DNSRecords.list()
      ch, // CustomHostnames.create()
      null, // CustomHostnames.list()
      // eslint-disable-next-line @typescript-eslint/naming-convention
      {custom_metadata: ch.custom_metadata}, // CustomHostnames.edit()
      record, // DNSRecords.create()
      null, // DNSRecords.list()
      record, // DNSRecords.update()
    ]);
  });

  test('deletes old hostname', async () => {
    const fetch = mockFetch()
      .result<Partial<DNSRecord>[]>(
        'GET',
        '/dns_records', // DNSRecords.list()
        [
          {
            name: 'foo.reflect-o-rama.net',
            id: 'foo-record-id',
            tags: ['ch:foo-ch-id'],
          },
          {
            name: 'bar.reflect-o-rama.net',
            id: 'bar-record-id',
            tags: ['ch:bar-ch-id'],
          },
        ],
      )
      .result('DELETE', '/custom_hostnames/bar-ch-id', {id: 'bar-ch-id'})
      .result('DELETE', '/dns_records/bar-record-id', {id: 'bar-record-id'});

    expect(await publish('foo.reflect-o-rama.net')).toEqual([]);

    expect(fetch.requests()).toEqual([
      [
        'GET',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records?tag=script%3Aprod%2Ffoo-script',
      ],
      [
        'DELETE',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/custom_hostnames/bar-ch-id',
      ],
      [
        'DELETE',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records/bar-record-id',
      ],
    ]);
  });

  test('delete is resumable', async () => {
    const fetch = mockFetch()
      .result<Partial<DNSRecord>[]>(
        'GET',
        '/dns_records', // DNSRecords.list()
        [
          {
            name: 'foo.reflect-o-rama.net',
            id: 'foo-record-id',
            tags: ['ch:foo-ch-id'],
          },
          {
            name: 'bar.reflect-o-rama.net',
            id: 'bar-record-id',
            tags: ['ch:bar-ch-id'],
          },
        ],
      )
      .error(
        'DELETE',
        '/custom_hostnames/bar-ch-id',
        Errors.CustomHostnameNotFound,
      )
      .error('DELETE', '/dns_records/bar-record-id', Errors.RecordDoesNotExist);

    expect(await publish('foo.reflect-o-rama.net')).toEqual([]);

    expect(fetch.requests()).toEqual([
      [
        'GET',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records?tag=script%3Aprod%2Ffoo-script',
      ],
      [
        'DELETE',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/custom_hostnames/bar-ch-id',
      ],
      [
        'DELETE',
        'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records/bar-record-id',
      ],
    ]);
  });

  test('create and delete hostnames', async () => {
    const fetch = mockFetch()
      .result<Partial<DNSRecord>[]>(
        'GET',
        '/dns_records', // DNSRecords.list()
        [{name: 'baz', id: 'baz-record-id', tags: ['ch:baz-ch-id']}],
      )
      .result('DELETE', '/custom_hostnames/baz-ch-id', {id: 'bar-ch-id'})
      .result('DELETE', '/dns_records/baz-record-id', {id: 'bar-record-id'})
      .result('POST', '/custom_hostnames', {id: 'ch-id', status: 'initial'}) // CustomHostnames.create()
      .result('POST', '/dns_records', {id: 'record-id'}) // DNSRecords.create()
      .result('PATCH', '/custom_hostnames/ch-id', {status: 'active'}); // CustomHostnames.edit()

    expect(await publish('foo.reflect-o-rama.net')).toEqual([
      'Setting up hostname foo.reflect-o-rama.net',
    ]);

    expect(fetch.spy).toHaveBeenCalledTimes(6);
    expect(fetch.requests()).toEqual(
      expect.arrayContaining([
        [
          'GET',
          'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records?tag=script%3Aprod%2Ffoo-script',
        ],
        [
          'DELETE',
          'https://api.cloudflare.com/client/v4/zones/1ab3d299c/custom_hostnames/baz-ch-id',
        ],
        [
          'DELETE',
          'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records/baz-record-id',
        ],
        [
          'POST',
          'https://api.cloudflare.com/client/v4/zones/1ab3d299c/custom_hostnames',
        ],
        [
          'POST',
          'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records',
        ],
        [
          'PATCH',
          'https://api.cloudflare.com/client/v4/zones/1ab3d299c/custom_hostnames/ch-id',
        ],
      ]),
    );
  });

  test('isolates errors per hostname', async () => {
    const UNEXPECTED_ERROR = 48583;
    const fetch = mockFetch()
      .result<Partial<DNSRecord>[]>(
        'GET',
        '/dns_records', // DNSRecords.list()
        [
          {
            name: 'foo.reflect-o-rama.net',
            id: 'foo-record-id',
            tags: ['ch:foo-ch-id'],
          },
          {
            name: 'bar.reflect-o-rama.net',
            id: 'bar-record-id',
            tags: ['ch:bar-ch-id'],
          },
          {
            name: 'baz.reflect-o-rama.net',
            id: 'baz-record-id',
            tags: ['ch:baz-ch-id'],
          },
        ],
      )
      // Delete of bar Custom Hostname fails.
      .error('DELETE', '/custom_hostnames/bar-ch-id', UNEXPECTED_ERROR)
      // Delete of baz Custom Hostname fails with already deleted.
      .error(
        'DELETE',
        '/custom_hostnames/baz-ch-id',
        Errors.CustomHostnameNotFound,
      )
      .result('DELETE', '/dns_records/baz-record-id', {id: 'bar-record-id'});

    let error;
    try {
      await publish('foo.reflect-o-rama.net');
    } catch (e) {
      error = e;
    }

    expect(error).not.toBeUndefined;
    expect(error).toBeInstanceOf(FetchResultError);
    expect((error as FetchResultError).code).toBe(UNEXPECTED_ERROR);

    expect(fetch.spy).toHaveBeenCalledTimes(4);
    expect(fetch.requests()).toEqual(
      expect.arrayContaining([
        [
          'GET',
          'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records?tag=script%3Aprod%2Ffoo-script',
        ],
        [
          'DELETE',
          'https://api.cloudflare.com/client/v4/zones/1ab3d299c/custom_hostnames/bar-ch-id',
        ],
        [
          'DELETE',
          'https://api.cloudflare.com/client/v4/zones/1ab3d299c/custom_hostnames/baz-ch-id',
        ],
        // DNS Record for baz is still deleted
        [
          'DELETE',
          'https://api.cloudflare.com/client/v4/zones/1ab3d299c/dns_records/baz-record-id',
        ],
      ]),
    );
  });
});
