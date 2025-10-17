import {expect, test} from 'vitest';
import {fakeHash} from '../hash.ts';
import * as KeyType from './key-type-enum.ts';
import {
  chunkDataKey,
  chunkMetaKey,
  chunkRefCountKey,
  headKey,
  type Key,
  parse,
} from './key.ts';

test('toString', () => {
  const hashEmptyString = fakeHash('');
  const hashA = fakeHash('a');
  const hashAB = fakeHash('ab');
  expect(chunkDataKey(hashEmptyString)).toBe(`c/${hashEmptyString}/d`);
  expect(chunkDataKey(hashA)).toBe(`c/${hashA}/d`);
  expect(chunkDataKey(hashAB)).toBe(`c/${hashAB}/d`);
  expect(chunkMetaKey(hashEmptyString)).toBe(`c/${hashEmptyString}/m`);
  expect(chunkMetaKey(hashA)).toBe(`c/${hashA}/m`);
  expect(chunkMetaKey(hashAB)).toBe(`c/${hashAB}/m`);
  expect(chunkRefCountKey(hashEmptyString)).toBe(`c/${hashEmptyString}/r`);
  expect(chunkRefCountKey(hashA)).toBe(`c/${hashA}/r`);
  expect(chunkRefCountKey(hashAB)).toBe(`c/${hashAB}/r`);
  expect(headKey('')).toBe(`h/`);
  expect(headKey('a')).toBe(`h/a`);
  expect(headKey('ab')).toBe(`h/ab`);
});

test('parse', () => {
  const hashA = fakeHash('a');
  const hashB = fakeHash('b');

  const t = (key: string, expected: Key) => {
    expect(parse(key)).toEqual(expected);
  };

  t(chunkDataKey(hashA), {type: KeyType.ChunkData, hash: hashA});
  t(chunkMetaKey(hashA), {type: KeyType.ChunkMeta, hash: hashA});
  t(chunkRefCountKey(hashA), {type: KeyType.ChunkRefCount, hash: hashA});
  t(headKey('a'), {type: KeyType.Head, name: 'a'});

  t(chunkDataKey(hashB), {type: KeyType.ChunkData, hash: hashB});
  t(chunkMetaKey(hashB), {type: KeyType.ChunkMeta, hash: hashB});
  t(chunkRefCountKey(hashB), {type: KeyType.ChunkRefCount, hash: hashB});
  t(headKey('b'), {type: KeyType.Head, name: 'b'});

  const invalid = (key: string, message: string) => {
    const fn = () => parse(key);
    expect(fn).toThrow(Error);
    try {
      fn();
    } catch (err) {
      expect(err).toHaveProperty('message', message);
    }
  };

  invalid('', `Invalid key. Got ""`);
  invalid('c', `Invalid key. Got "c"`);
  invalid('c/', `Invalid key. Got "c/"`);
  invalid('c/abc', `Invalid key. Got "c/abc"`);
  invalid('c/abc/', `Invalid key. Got "c/abc/"`);
  invalid('c/abc/x', `Invalid key. Got "c/abc/x"`);

  invalid('c//d', `Invalid hash. Got ""`);
  invalid('c//m', `Invalid hash. Got ""`);
  invalid('c//r', `Invalid hash. Got ""`);

  invalid('c/d', `Invalid key. Got "c/d"`);
  invalid('c/m', `Invalid key. Got "c/m"`);
  invalid('c/r', `Invalid key. Got "c/r"`);
});
