import {expect, test} from 'vitest';
import {PeekIterator} from '../util/peek-iterator.ts';

test('PeekIterator', () => {
  const c = new PeekIterator('abc'[Symbol.iterator]());
  expect(c.peek().value).toBe('a');
  expect(c.peek().value).toBe('a');
  expect(c.next().value).toBe('a');
  expect(c.peek().value).toBe('b');
  expect(c.peek().value).toBe('b');
  expect(c.next().value).toBe('b');
  expect(c.peek().value).toBe('c');
  expect(c.peek().value).toBe('c');
  expect(c.next().value).toBe('c');
  expect(c.peek().done);
  expect(c.peek().done);
  expect(c.next().done);
});
