import {describe, expect, jest, test} from '@jest/globals';
import {SecretsCache, SecretsClient} from './index.js';

describe('SecretsCache', () => {
  function mockClient() {
    return {
      fetchSecret: jest.fn().mockImplementation((name, version) =>
        version === 'non-existent'
          ? Promise.reject(new Error('non-existent secret'))
          : Promise.resolve({
              version: version === 'latest' ? '3' : version,
              payload: `${name}-payload`,
            }),
      ),
    };
  }

  test('caches fetched values', async () => {
    const client = mockClient();
    const cache = new SecretsCache(client as unknown as SecretsClient);

    const results = await Promise.all([
      cache.getSecret('foo'),
      cache.getSecret('bar', '2'),
      cache.getSecret('foo', 'latest'),
      cache.getSecret('bar', '2'),
    ]);

    expect(results).toEqual([
      {version: '3', payload: 'foo-payload'},
      {version: '2', payload: 'bar-payload'},
      {version: '3', payload: 'foo-payload'},
      {version: '2', payload: 'bar-payload'},
    ]);

    expect(client.fetchSecret).toBeCalledTimes(2);

    expect(await cache.getSecret('foo')).toEqual({
      version: '3',
      payload: 'foo-payload',
    });
    expect(await cache.getSecret('foo', 'latest')).toEqual({
      version: '3',
      payload: 'foo-payload',
    });
    expect(await cache.getSecret('foo', '3')).toEqual({
      version: '3',
      payload: 'foo-payload',
    });
    expect(await cache.getSecret('bar', '2')).toEqual({
      version: '2',
      payload: 'bar-payload',
    });

    // Should have returned from the cache and not the client.
    expect(client.fetchSecret).toBeCalledTimes(2);

    expect(await cache.getSecret('bar')).toEqual({
      version: '3',
      payload: 'bar-payload',
    });

    // Novel versions should consult the client.
    expect(client.fetchSecret).toBeCalledTimes(3);

    expect(await cache.getSecret('foo', '2')).toEqual({
      version: '2',
      payload: 'foo-payload',
    });

    // Novel versions should consult the client.
    expect(client.fetchSecret).toBeCalledTimes(4);
  });

  test('caches missing value Errors', async () => {
    const client = mockClient();
    const cache = new SecretsCache(client as unknown as SecretsClient);

    expect(
      await cache.getSecret('foo', 'non-existent').catch(e => e),
    ).toBeInstanceOf(Error);

    expect(client.fetchSecret).toBeCalledTimes(1);

    expect(
      await cache.getSecret('foo', 'non-existent').catch(e => e),
    ).toBeInstanceOf(Error);

    // Error is cached for version "non-existent".
    expect(client.fetchSecret).toBeCalledTimes(1);

    expect(await cache.getSecret('foo')).toEqual({
      version: '3',
      payload: 'foo-payload',
    });

    expect(client.fetchSecret).toBeCalledTimes(2);
  });
});
