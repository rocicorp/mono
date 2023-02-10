import {test, expect} from '@jest/globals';
import {PeekIterator} from '../../src/util/peek-iterator.js';

test('PeekIterator', () => {
  const c = new PeekIterator('abc'[Symbol.iterator]());
  expect(c.peek().value).toEqual('a');
  expect(c.peek().value).toEqual('a');
  expect(c.next().value).toEqual('a');
  expect(c.peek().value).toEqual('b');
  expect(c.peek().value).toEqual('b');
  expect(c.next().value).toEqual('b');
  expect(c.peek().value).toEqual('c');
  expect(c.peek().value).toEqual('c');
  expect(c.next().value).toEqual('c');
  expect(c.peek().done);
  expect(c.peek().done);
  expect(c.next().done);
});
