import {expect} from '@esm-bundle/chai';
import {Hash, fakeHash, parse} from '../hash';
import type {Value} from '../kv/store';
import {createChunk, createChunkWithHash, makeTestChunkHasher} from './chunk';
import type {Chunk} from './chunk';

test('round trip', async () => {
  const chunkHasher = makeTestChunkHasher();
  const t = (hash: Hash, data: Value, refs: Hash[]) => {
    const c = createChunk(data, refs, chunkHasher);
    expect(c.hash).to.equal(hash);
    expect(c.data).to.deep.equal(data);
    expect(c.meta).to.deep.equal(refs);

    const buf = c.meta;
    const c2 = createChunkWithHash(hash, data, buf);
    expect(c).to.deep.equal(c2);
  };

  t(parse('fake0000000000000000000000000000'), [], []);
  t(parse('fake0000000000000000000000000001'), [0], [fakeHash('r1')]);
  t(
    parse('fake0000000000000000000000000002'),
    [0, 1],
    [fakeHash('r1'), fakeHash('r2')],
  );
});

test('equals', async () => {
  const eq = (a: Chunk, b: Chunk) => {
    expect(a).to.deep.equal(b);
  };

  const neq = (a: Chunk, b: Chunk) => {
    expect(a).to.not.deep.equal(b);
  };

  const chunkHasher = makeTestChunkHasher('fake');

  const newChunk = (data: Value, refs: Hash[]) => {
    return createChunk(data, refs, chunkHasher);
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
