import {expect} from 'chai';
import {fakeHash} from '../hash.js';
import {
  chunkDataKey,
  chunkMetaKey,
  chunkRefCountKey,
  headKey,
  Key,
  KeyType,
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

  invalid('', `Invalid key: ''`);
  invalid('c', `Invalid key: 'c'`);
  invalid('c/', `Invalid key: 'c/'`);
  invalid('c/abc', `Invalid key: 'c/abc'`);
  invalid('c/abc/', `Invalid key: 'c/abc/'`);
  invalid('c/abc/x', `Invalid key: 'c/abc/x'`);

  invalid('c//d', `Invalid hash: ''`);
  invalid('c//m', `Invalid hash: ''`);
  invalid('c//r', `Invalid hash: ''`);

  invalid('c/d', `Invalid key: 'c/d'`);
  invalid('c/m', `Invalid key: 'c/m'`);
  invalid('c/r', `Invalid key: 'c/r'`);
});
