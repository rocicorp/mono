import {afterEach, expect, jest, test} from '@jest/globals';
import {Secrets, SecretsCache} from './index.js';

const client = {
  getSecret: jest.fn().mockImplementation((name, version) =>
    Promise.resolve({
      version: version === 'latest' ? '3' : version,
      payload: `${name}-payload`,
    }),
  ),
};

afterEach(() => {
  jest.resetAllMocks();
});

test('SecretsCache', async () => {
  const cache = new SecretsCache(client as unknown as Secrets);

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

  expect(client.getSecret).toBeCalledTimes(2);

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
  expect(client.getSecret).toBeCalledTimes(2);

  expect(await cache.getSecret('bar')).toEqual({
    version: '3',
    payload: 'bar-payload',
  });

  // Novel versions should consult the client.
  expect(client.getSecret).toBeCalledTimes(3);

  expect(await cache.getSecret('foo', '2')).toEqual({
    version: '2',
    payload: 'foo-payload',
  });

  // Novel versions should consult the client.
  expect(client.getSecret).toBeCalledTimes(4);
});
