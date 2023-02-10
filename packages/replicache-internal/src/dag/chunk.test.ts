import {expect} from '@esm-bundle/chai';
import {Hash, fakeHash, parse, makeNewFakeHashFunction} from '../hash';
import {createChunk, createChunkWithHash} from './chunk';
import type {Chunk} from './chunk';
import type {ReadonlyJSONValue} from '../json';

test('round trip', () => {
  const chunkHasher = makeNewFakeHashFunction();
  const t = (hash: Hash, data: ReadonlyJSONValue, refs: Hash[]) => {
    const c = createChunk(data, refs, chunkHasher);
    expect(c.hash).to.equal(hash);
    expect(c.data).to.deep.equal(data);
    expect(c.meta).to.deep.equal(refs);

    const buf = c.meta;
    const c2 = createChunkWithHash(hash, data, buf);
    expect(c).to.deep.equal(c2);
  };

  t(parse('face0000000040008000000000000000' + '000000000000'), [], []);
  t(
    parse('face0000000040008000000000000000' + '000000000001'),
    [0],
    [fakeHash('a1')],
  );
  t(
    parse('face0000000040008000000000000000' + '000000000002'),
    [0, 1],
    [fakeHash('a1'), fakeHash('a2')],
  );
});

test('equals', () => {
  const eq = (a: Chunk, b: Chunk) => {
    expect(a).to.deep.equal(b);
  };

  const neq = (a: Chunk, b: Chunk) => {
    expect(a).to.not.deep.equal(b);
  };

  const chunkHasher = makeNewFakeHashFunction();

  const hashMapper: Map<string, Hash> = new Map();

  const newChunk = (data: ReadonlyJSONValue, refs: Hash[]) => {
    // Cache chunks based on the data.
    // TODO(arv): This is not very useful any more... Remove?
    const s = JSON.stringify(data);
    let hash = hashMapper.get(s);
    if (!hash) {
      hash = chunkHasher();
      hashMapper.set(s, hash);
    }

    return createChunkWithHash(hash, data, refs);
  };

  eq(newChunk([], []), newChunk([], []));
  neq(newChunk([1], []), newChunk([], []));
  neq(newChunk([0], []), newChunk([1], []));

  eq(newChunk([1], []), newChunk([1], []));
  eq(newChunk([], [fakeHash('a')]), newChunk([], [fakeHash('a')]));

  neq(newChunk([], [fakeHash('a')]), newChunk([], [fakeHash('b')]));
  neq(
    newChunk([], [fakeHash('a')]),
    newChunk([], [fakeHash('a'), fakeHash('b')]),
  );
});
