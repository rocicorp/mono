import {expect, test} from 'vitest';
import {TupleMap} from './tuple-map.ts';

test('get and set', () => {
  const map = new TupleMap<readonly [string, number], string>();
  expect(map.get(['a', 1])).toBeUndefined();
  expect(map.set(['a', 1], 'a1')).toBe(map);
  map.set(['a', 2], 'a2').set(['b', 1], 'b1');
  expect(map.get(['a', 1])).toBe('a1');
  expect(map.get(['a', 2])).toBe('a2');
  expect(map.get(['b', 1])).toBe('b1');
  expect(map.get(['b', 2])).toBeUndefined();

  map.set(['a', 1], 'changed');
  expect(map.get(['a', 1])).toBe('changed');
});

test('elements are compared like the keys of a Map', () => {
  const map = new TupleMap<readonly unknown[], number>();
  const o = {};
  map.set([o, undefined], 1);
  map.set([NaN, null], 2);
  map.set(['1'], 3);
  expect(map.get([o, undefined])).toBe(1);
  expect(map.get([{}, undefined])).toBeUndefined();
  expect(map.get([o, null])).toBeUndefined();
  expect(map.get([NaN, null])).toBe(2);
  expect(map.get(['1'])).toBe(3);
  expect(map.get([1])).toBeUndefined();
});

test('a key and its prefixes are different keys', () => {
  const map = new TupleMap<readonly string[], number>();
  map.set(['a', 'b'], 2);
  expect(map.get([])).toBeUndefined();
  expect(map.get(['a'])).toBeUndefined();
  expect(map.get(['a', 'b', 'c'])).toBeUndefined();

  map.set([], 0).set(['a'], 1);
  expect(map.get([])).toBe(0);
  expect(map.get(['a'])).toBe(1);
  expect(map.get(['a', 'b'])).toBe(2);
});

test('getOrInsertComputed', () => {
  const map = new TupleMap<readonly [string, string], string | null>();
  const computed: (readonly [string, string])[] = [];
  const compute = (key: readonly [string, string]) => {
    computed.push(key);
    return key.join('');
  };
  expect(map.getOrInsertComputed(['a', 'b'], compute)).toBe('ab');
  expect(map.getOrInsertComputed(['a', 'b'], compute)).toBe('ab');
  expect(map.getOrInsertComputed(['a', 'c'], compute)).toBe('ac');
  expect(computed).toEqual([
    ['a', 'b'],
    ['a', 'c'],
  ]);
  expect(map.get(['a', 'b'])).toBe('ab');

  // A value that is already there is kept, even `null`.
  map.set(['n', 'n'], null);
  expect(map.getOrInsertComputed(['n', 'n'], compute)).toBeNull();
  expect(computed).toHaveLength(2);
});
