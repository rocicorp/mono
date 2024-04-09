import {expect, test} from 'vitest';
import {DifferenceIndex} from './difference-index.js';
import {joinSymbol} from '../../types.js';

test('get', () => {
  const index = new DifferenceIndex<string, number>(x => x);
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
  const index = new DifferenceIndex<string, number>(x => x);
  index.add('a', [1, 1]);
  index.add('a', [1, 1]);
  index.add('a', [2, 1]);
  index.add('b', [3, 2]);
  index.compact(['a', 'b']);

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

  index.compact(['b']);
  expect(index.get('a')).toEqual([
    [1, 2],
    [2, 1],
    [1, -1],
  ]);

  index.compact(['a']);
  expect(index.get('a')).toEqual([
    [1, 1],
    [2, 1],
  ]);

  index.add('a', [1, -1]);
  index.add('a', [2, -1]);
  index.add('a', [1, -1]);
  index.compact(['a']);

  expect(index.get('a')).toEqual([[1, -1]]);

  index.add('a', [1, 1]);
  index.compact(['a']);
  expect(index.get('a')).toEqual([]);
});

const identity = <T>(x: T) => x;
test('join', () => {
  const indexA = new DifferenceIndex<string, number>(identity);
  const indexB = new DifferenceIndex<string, number>(identity);

  let result = indexA.join('a', indexB, 'b', identity);
  expect(result).toEqual([]);

  indexA.add('a', [1, 1]);
  result = indexA.join('a', indexB, 'b', identity);
  expect(result).toEqual([]);

  indexB.add('a', [1, 1]);
  result = indexA.join('a', indexB, 'b', identity);
  expect(result).toEqual([[{id: '1_1', a: 1, b: 1, [joinSymbol]: true}, 1]]);

  indexA.add('a', [1, 1]);
  result = indexA.join('a', indexB, 'b', identity);
  expect(result).toEqual([
    [{id: '1_1', a: 1, b: 1, [joinSymbol]: true}, 1],
    [{id: '1_1', a: 1, b: 1, [joinSymbol]: true}, 1],
  ]);

  indexA.add('b', [2, 1]);
  result = indexA.join('a', indexB, 'b', identity);
  expect(result).toEqual([
    [{id: '1_1', a: 1, b: 1, [joinSymbol]: true}, 1],
    [{id: '1_1', a: 1, b: 1, [joinSymbol]: true}, 1],
  ]);

  indexB.add('b', [2, 1]);
  result = indexA.join('a', indexB, 'b', identity);
  expect(result).toEqual([
    [{id: '1_1', a: 1, b: 1, [joinSymbol]: true}, 1],
    [{id: '1_1', a: 1, b: 1, [joinSymbol]: true}, 1],
    [{id: '2_2', a: 2, b: 2, [joinSymbol]: true}, 1],
  ]);

  indexA.add('a', [1, -1]);
  result = indexA.join('a', indexB, 'b', identity);
  expect(result).toEqual([
    [{id: '1_1', a: 1, b: 1, [joinSymbol]: true}, 1],
    [{id: '1_1', a: 1, b: 1, [joinSymbol]: true}, 1],
    [{id: '1_1', a: 1, b: 1, [joinSymbol]: true}, -1],
    [{id: '2_2', a: 2, b: 2, [joinSymbol]: true}, 1],
  ]);

  indexA.compact(['a']);
  result = indexA.join('a', indexB, 'b', identity);
  expect(result).toEqual([
    [{id: '1_1', a: 1, b: 1, [joinSymbol]: true}, 1],
    [{id: '2_2', a: 2, b: 2, [joinSymbol]: true}, 1],
  ]);

  indexB.add('b', [2, -1]);
  result = indexA.join('a', indexB, 'b', identity);
  expect(result).toEqual([
    [{id: '1_1', a: 1, b: 1, [joinSymbol]: true}, 1],
    [{id: '2_2', a: 2, b: 2, [joinSymbol]: true}, 1],
    [{id: '2_2', a: 2, b: 2, [joinSymbol]: true}, -1],
  ]);

  indexB.compact(['b']);
  result = indexA.join('a', indexB, 'b', identity);
  expect(result).toEqual([[{id: '1_1', a: 1, b: 1, [joinSymbol]: true}, 1]]);
});
