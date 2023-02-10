import {PeekIterator} from '../util/peek-iterator.js';
import {expect} from '@esm-bundle/chai';

test('PeekIterator', () => {
  const c = new PeekIterator('abc'[Symbol.iterator]());
  expect(c.peek().value).equal('a');
  expect(c.peek().value).equal('a');
  expect(c.next().value).equal('a');
  expect(c.peek().value).equal('b');
  expect(c.peek().value).equal('b');
  expect(c.next().value).equal('b');
  expect(c.peek().value).equal('c');
  expect(c.peek().value).equal('c');
  expect(c.next().value).equal('c');
  expect(c.peek().done);
  expect(c.peek().done);
  expect(c.next().done);
});
