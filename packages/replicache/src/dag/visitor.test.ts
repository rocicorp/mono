/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members, @typescript-eslint/prefer-promise-reject-errors */
import {expect, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import {type Hash, fakeHash} from '../hash.ts';
import {Chunk, toRefs} from './chunk.ts';
import type {MustGetChunk} from './store.ts';
import {Visitor} from './visitor.ts';

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
