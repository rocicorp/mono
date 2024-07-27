import {expect, test} from 'vitest';
import {MemoryBackedDifferenceIndex} from './difference-index.js';

test('get', () => {
  const index = new MemoryBackedDifferenceIndex<string, number>(x => x);
  index.add('a', [1, 1]);
  index.add('a', [1, 1]);
  index.add('a', [2, 1]);
  index.add('b', [3, 2]);

  expect(index.get('a')).toEqual([
    [1, 1],
    [1, 1],
    [2, 1],
  ]);
  expect(index.get('b')).toEqual([[3, 2]]);
});

test('compact', () => {
  const index = new MemoryBackedDifferenceIndex<string, number>(x => x);
  index.add('a', [1, 1]);
  index.add('a', [1, 1]);
  index.add('a', [2, 1]);
  index.add('b', [3, 2]);
  index.compact();

  expect(index.get('a')).toEqual([
    [1, 2],
    [2, 1],
  ]);
  expect(index.get('b')).toEqual([[3, 2]]);

  index.add('a', [1, -1]);
  expect(index.get('a')).toEqual([
    [1, 2],
    [2, 1],
    [1, -1],
  ]);

  index.compact();
  expect(index.get('a')).toEqual([
    [1, 1],
    [2, 1],
  ]);

  index.compact();
  expect(index.get('a')).toEqual([
    [1, 1],
    [2, 1],
  ]);

  index.add('a', [1, -1]);
  index.add('a', [2, -1]);
  index.add('a', [1, -1]);
  index.compact();

  expect(index.get('a')).toEqual([[1, -1]]);

  index.add('a', [1, 1]);
  index.compact();
  expect(index.get('a')).toEqual(undefined);
});
