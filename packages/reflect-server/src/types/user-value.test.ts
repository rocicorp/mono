import {test, expect} from '@jest/globals';
import {
  decodeUserValueVersionKey,
  userValueVersionEntry,
  userValueVersionKey,
} from './user-value.js';

type Case = {
  key: string;
  version: number;
  indexKey: string;
};
const cases: Case[] = [
  {
    key: '',
    version: 0,
    indexKey: 'v/00/',
  },
  {
    key: 'foo',
    version: 1234,
    indexKey: 'v/1ya/foo',
  },
  {
    key: 'user/key/with/slashes',
    version: 12345,
    indexKey: 'v/29ix/user/key/with/slashes',
  },
  {
    key: '/user/key/starting/with/a/slash',
    version: 54321,
    indexKey: 'v/315wx//user/key/starting/with/a/slash',
  },
];

test('version key encoding', () => {
  for (const c of cases) {
    expect(userValueVersionKey(c.key, c.version)).toBe(c.indexKey);
  }
});

test('version key decoding', () => {
  for (const c of cases) {
    expect(decodeUserValueVersionKey(c.indexKey)).toEqual({
      userKey: c.key,
      version: c.version,
    });
  }
});

test('version entries', () => {
  for (const c of cases) {
    // put
    expect(
      userValueVersionEntry(c.key, {
        version: c.version,
        deleted: false,
        value: {foo: 'bar'},
      }),
    ).toEqual({
      key: c.indexKey,
      value: {},
    });

    // del
    expect(
      userValueVersionEntry(c.key, {
        version: c.version,
        deleted: true,
        value: {foo: 'bar'},
      }),
    ).toEqual({
      key: c.indexKey,
      value: {deleted: true},
    });
  }
});
