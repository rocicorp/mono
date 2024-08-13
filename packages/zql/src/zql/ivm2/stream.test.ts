import {expect, test} from 'vitest';
import {makeStream} from './stream.js';

test('once', () => {
  const arr = [1, 2, 3];
  const cs = makeStream(arr);
  expect([...cs]).toEqual(arr);
  expect([...cs]).toEqual([]);
});
