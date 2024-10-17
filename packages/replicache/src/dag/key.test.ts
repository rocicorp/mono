import {expect, test} from 'vitest';
import {fakeHash} from '../hash.js';
import * as KeyType from './key-type-enum.js';
import {
  chunkDataKey,
  chunkMetaKey,
  chunkRefCountKey,
  headKey,
  type Key,
  parse,
} from './key.js';

test('toString', () => {
  const hashEmptyString = fakeHash('');
  const hashA = fakeHash('a');
  const hashAB = fakeHash('ab');
  expect(chunkDataKey(hashEmptyString)).to.equal(`c/${hashEmptyString}/d`);
  expect(chunkDataKey(hashA)).to.equal(`c/${hashA}/d`);
  expect(chunkDataKey(hashAB)).to.equal(`c/${hashAB}/d`);
  expect(chunkMetaKey(hashEmptyString)).to.equal(`c/${hashEmptyString}/m`);
  expect(chunkMetaKey(hashA)).to.equal(`c/${hashA}/m`);
  expect(chunkMetaKey(hashAB)).to.equal(`c/${hashAB}/m`);
  expect(chunkRefCountKey(hashEmptyString)).to.equal(`c/${hashEmptyString}/r`);
  expect(chunkRefCountKey(hashA)).to.equal(`c/${hashA}/r`);
  expect(chunkRefCountKey(hashAB)).to.equal(`c/${hashAB}/r`);
  expect(headKey('')).to.equal(`h/`);
  expect(headKey('a')).to.equal(`h/a`);
  expect(headKey('ab')).to.equal(`h/ab`);
});

test('parse', () => {
  const hashA = fakeHash('a');
  const hashB = fakeHash('b');

  const t = (key: string, expected: Key) => {
    expect(parse(key)).to.deep.equal(expected);
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
    expect(() => parse(key))
      .to.throw(Error)
      .with.property('message', message);
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
