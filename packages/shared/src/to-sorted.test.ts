import {expect, test} from 'vitest';
import {getESLibVersion} from './get-es-lib-version.ts';
import {toSorted} from './to-sorted.ts';

test('lib < ES2023', () => {
  // toSorted was added in ES2023

  // sanity check that we are using es2022. If this starts failing then we can
  // remove the toSorted and use the builtin.
  expect(getESLibVersion()).toBeLessThan(2023);
});

test('sorts an array of numbers in ascending order', () => {
  const array = [3, 1, 4, 1, 5, 9];
  const sorted = toSorted(array, (a, b) => a - b);
  expect(sorted).toEqual([1, 1, 3, 4, 5, 9]);
  // Original array should not be modified
  expect(array).toEqual([3, 1, 4, 1, 5, 9]);
});

test('sorts an array of numbers in descending order', () => {
  const array = [3, 1, 4, 1, 5, 9];
  const sorted = toSorted(array, (a, b) => b - a);
  expect(sorted).toEqual([9, 5, 4, 3, 1, 1]);
  expect(array).toEqual([3, 1, 4, 1, 5, 9]);
});

test('sorts an array of strings alphabetically', () => {
  const array = ['banana', 'apple', 'cherry', 'date'];
  const sorted = toSorted(array, (a, b) => a.localeCompare(b));
  expect(sorted).toEqual(['apple', 'banana', 'cherry', 'date']);
  expect(array).toEqual(['banana', 'apple', 'cherry', 'date']);
});

test('handles empty array', () => {
  const array: number[] = [];
  const sorted = toSorted(array, (a, b) => a - b);
  expect(sorted).toEqual([]);
  expect(array).toEqual([]);
});

test('works with array of objects', () => {
  const array = [
    {name: 'John', age: 30},
    {name: 'Alice', age: 25},
    {name: 'Bob', age: 40},
  ];
  const sorted = toSorted(array, (a, b) => a.age - b.age);
  expect(sorted).toEqual([
    {name: 'Alice', age: 25},
    {name: 'John', age: 30},
    {name: 'Bob', age: 40},
  ]);
  expect(array).toEqual([
    {name: 'John', age: 30},
    {name: 'Alice', age: 25},
    {name: 'Bob', age: 40},
  ]);
});

test('returns a new array instance', () => {
  const array = [3, 1, 4];
  const sorted = toSorted(array, (a, b) => a - b);
  expect(sorted).not.toBe(array);
});

test('compare is optional', () => {
  const array = ['c', 'a', 'd'];
  const sorted = toSorted(array);
  expect(sorted).toEqual(['a', 'c', 'd']);
});

test('compare is optional and uses funky default compare', () => {
  const array = [33, 2, 111];
  const sorted = toSorted(array);
  expect(sorted).toEqual([111, 2, 33]);
});
