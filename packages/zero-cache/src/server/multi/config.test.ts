import {expect, test} from 'vitest';
import {getMultiZeroConfig} from './config.js';

test('env merging', () => {
  expect(
    getMultiZeroConfig({}, [
      '--tenant-configs-json',
      JSON.stringify({
        baseEnv: {
          ['ZERO_UPSTREAM_DB']: 'foo',
          ['ZERO_CVR_DB']: 'foo',
          ['ZERO_CHANGE_DB']: 'foo',
        },
        tenants: [
          {
            name: 'tenboo',
            host: 'Normalize.ME',
            path: 'tenboo',
            env: {['ZERO_REPLICA_FILE']: 'tenboo.db'},
          },
          {
            name: 'tenbar',
            path: '/tenbar',
            env: {['ZERO_REPLICA_FILE']: 'tenbar.db'},
          },
          {
            name: 'tenbaz',
            path: '/tenbaz',
            env: {
              ['ZERO_REPLICA_FILE']: 'tenbar.db',
              ['ZERO_CHANGE_DB']: 'overridden',
            },
          },
        ],
      }),
    ]),
  ).toMatchInlineSnapshot(`
    {
      "log": {
        "format": "text",
        "level": "info",
      },
      "port": 4848,
      "tenants": [
        {
          "env": {
            "ZERO_CHANGE_DB": "foo",
            "ZERO_CVR_DB": "foo",
            "ZERO_REPLICA_FILE": "tenboo.db",
            "ZERO_UPSTREAM_DB": "foo",
          },
          "host": "normalize.me",
          "name": "tenboo",
          "path": "/tenboo",
        },
        {
          "env": {
            "ZERO_CHANGE_DB": "foo",
            "ZERO_CVR_DB": "foo",
            "ZERO_REPLICA_FILE": "tenbar.db",
            "ZERO_UPSTREAM_DB": "foo",
          },
          "name": "tenbar",
          "path": "/tenbar",
        },
        {
          "env": {
            "ZERO_CHANGE_DB": "overridden",
            "ZERO_CVR_DB": "foo",
            "ZERO_REPLICA_FILE": "tenbar.db",
            "ZERO_UPSTREAM_DB": "foo",
          },
          "name": "tenbaz",
          "path": "/tenbaz",
        },
      ],
    }
  `);
});

test.each([
  [
    'Missing property ZERO_REPLICA_FILE',
    {
      name: 'tenboo',
      path: '/tenboo',
      env: {},
    },
  ],
  [
    'Tenant "tenboo" is missing a host or path field',
    {
      name: 'tenboo',
      env: {['ZERO_REPLICA_FILE']: 'foo.db'},
    },
  ],
  [
    'Only a single path component may be specified',
    {
      name: 'tenboo',
      path: '/too/many-slashes',
      env: {['ZERO_REPLICA_FILE']: 'foo.db'},
    },
  ],
])('%s', (errMsg, tenant) => {
  expect(() =>
    getMultiZeroConfig({}, [
      '--tenant-configs-json',
      JSON.stringify({
        baseEnv: {
          ['ZERO_UPSTREAM_DB']: 'foo',
          ['ZERO_CVR_DB']: 'foo',
          ['ZERO_CHANGE_DB']: 'foo',
        },
        tenants: [tenant],
      }),
    ]),
  ).toThrowError(errMsg);
});
