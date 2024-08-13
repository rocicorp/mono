import {expect, test} from 'vitest';
import {makeStream} from './stream.js';
import type {Node} from './data.js';

function makeNode(id: number): Node {
  return {row: {id}, relationships: new Map()};
}

test('once', () => {
  const arr = [...[1, 2, 3].map(makeNode)];
  const cs = makeStream(arr);
  expect([...cs]).toEqual(arr);
  expect([...cs]).toEqual([]);
});
