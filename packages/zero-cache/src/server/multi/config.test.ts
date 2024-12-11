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
            id: 'tenboo',
            host: 'Normalize.ME',
            path: 'tenboo',
            env: {['ZERO_REPLICA_FILE']: 'tenboo.db'},
          },
          {
            id: 'tenbar',
            path: '/tenbar',
            env: {['ZERO_REPLICA_FILE']: 'tenbar.db'},
          },
          {
            id: 'tenbaz',
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
          "id": "tenboo",
          "path": "/tenboo",
        },
        {
          "env": {
            "ZERO_CHANGE_DB": "foo",
            "ZERO_CVR_DB": "foo",
            "ZERO_REPLICA_FILE": "tenbar.db",
            "ZERO_UPSTREAM_DB": "foo",
          },
          "id": "tenbar",
          "path": "/tenbar",
        },
        {
          "env": {
            "ZERO_CHANGE_DB": "overridden",
            "ZERO_CVR_DB": "foo",
            "ZERO_REPLICA_FILE": "tenbar.db",
            "ZERO_UPSTREAM_DB": "foo",
          },
          "id": "tenbaz",
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
      id: 'tenboo',
      path: '/tenboo',
      env: {},
    },
  ],
  [
    'Tenant "tenboo" is missing a host or path field',
    {
      id: 'tenboo',
      env: {['ZERO_REPLICA_FILE']: 'foo.db'},
    },
  ],
  [
    'Only a single path component may be specified',
    {
      id: 'tenboo',
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
