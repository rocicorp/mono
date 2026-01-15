import {expect, test} from 'vitest';
import {findLastIndex} from './find-last-index.ts';
import {getESLibVersion} from './get-es-lib-version.ts';

test('lib < ES2023', () => {
  // findLastIndex was added in ES2023

  // sanity check that we are using es2022. If this starts failing then we can
  // remove the findLastIndex and use the builtin.
  expect(getESLibVersion()).toBeLessThan(2023);
});

test('finds the last element that satisfies the predicate', () => {
  const array = [1, 2, 3, 4, 5];
  const index = findLastIndex(array, num => num % 2 === 0);
  expect(index).toBe(3);
});

test('returns -1 when no element satisfies the predicate', () => {
  const array = [1, 3, 5, 7, 9];
  const index = findLastIndex(array, num => num % 2 === 0);
  expect(index).toBe(-1);
});

test('returns -1 for an empty array', () => {
  const array: number[] = [];
  const index = findLastIndex(array, () => true);
  expect(index).toBe(-1);
});

test('finds the last occurrence of a value', () => {
  const array = [1, 3, 5, 3, 1];
  const index = findLastIndex(array, num => num === 3);
  expect(index).toBe(3);
});

test('works with objects', () => {
  const array = [
    {id: 1, active: true},
    {id: 2, active: false},
    {id: 3, active: true},
    {id: 4, active: false},
  ];
  const index = findLastIndex(array, obj => obj.active);
  expect(index).toBe(2);
});

test('provides correct index to predicate function', () => {
  const array = ['a', 'b', 'c', 'd'];
  const receivedIndices: number[] = [];

  findLastIndex(array, (_, index) => {
    receivedIndices.unshift(index);
    return false;
  });

  expect(receivedIndices).toEqual([0, 1, 2, 3]);
});

test('works with predicates returning truthy/falsy values', () => {
  const array = [{name: 'a'}, {name: ''}, {name: 'b'}, {name: null}];
  // Predicate returns string | null, not boolean
  const index = findLastIndex(array, obj => obj.name);
  expect(index).toBe(2);
});
