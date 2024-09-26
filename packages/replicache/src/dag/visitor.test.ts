import {expect} from 'chai';
import {assert} from 'shared/src/asserts.js';
import {type Hash, fakeHash} from '../hash.js';
import {Chunk, toRefs} from './chunk.js';
import type {MustGetChunk} from './store.js';
import {Visitor} from './visitor.js';

test('Ensure only visited once', async () => {
  const c1 = new Chunk(fakeHash('1'), 'data1', []);
  const c2 = new Chunk(fakeHash('2'), 'data2', [c1.hash]);
  const c3 = new Chunk(fakeHash('3'), 'data3', toRefs([c1.hash, c2.hash]));

  const log: Chunk[] = [];
  class TestVisitor extends Visitor {
    visitChunk(chunk: Chunk) {
      log.push(chunk);
      return super.visitChunk(chunk);
    }
  }

  const chunks = new Map([
    [c1.hash, c1],
    [c2.hash, c2],
    [c3.hash, c3],
  ]);

  const dagRead: MustGetChunk = {
    mustGetChunk(h: Hash) {
      const chunk = chunks.get(h);
      assert(chunk);
      return Promise.resolve(chunk);
    },
  };

  const v = new TestVisitor(dagRead);
  await v.visit(c3.hash);

  expect(log).to.deep.equal([c3, c1, c2]);
});
