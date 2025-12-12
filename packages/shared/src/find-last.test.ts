import {expect, test} from 'vitest';
import data from '../../../tsconfig.json' with {type: 'json'};
import {findLast} from './find-last.ts';

function getESLibVersion(libs: string[]): number {
  const esVersion = libs.find(lib => lib.toLowerCase().startsWith('es'));
  if (!esVersion) {
    throw new Error('Could not find ES lib version');
  }
  return parseInt(esVersion.slice(2), 10);
}

test('lib < ES2023', () => {
  // findLast was added in ES2023

  // sanity check that we are using es2022. If this starts failing then we can
  // remove the findLast and use the builtin.
  expect(getESLibVersion(data.compilerOptions.lib)).toBeLessThan(2023);
});

test('finds the last element that satisfies the predicate', () => {
  const array = [1, 2, 3, 4, 5];
  const result = findLast(array, num => num % 2 === 0);
  expect(result).toBe(4);
});

test('returns undefined when no element satisfies the predicate', () => {
  const array = [1, 3, 5, 7, 9];
  const result = findLast(array, num => num % 2 === 0);
  expect(result).toBe(undefined);
});

test('returns undefined for an empty array', () => {
  const array: number[] = [];
  const result = findLast(array, () => true);
  expect(result).toBe(undefined);
});

test('finds the last occurrence of a value', () => {
  const array = [1, 3, 5, 3, 1];
  const result = findLast(array, num => num === 3);
  expect(result).toBe(3);
});

test('works with objects', () => {
  const array = [
    {id: 1, active: true},
    {id: 2, active: false},
    {id: 3, active: true},
    {id: 4, active: false},
  ];
  const result = findLast(array, obj => obj.active);
  expect(result).toEqual({id: 3, active: true});
});

test('provides correct index to predicate function', () => {
  const array = ['a', 'b', 'c', 'd'];
  const receivedIndices: number[] = [];

  findLast(array, (_, index) => {
    receivedIndices.unshift(index);
    return false;
  });

  expect(receivedIndices).toEqual([0, 1, 2, 3]);
});

test('works with predicates returning truthy/falsy values', () => {
  const array = [{name: 'a'}, {name: ''}, {name: 'b'}, {name: null}];
  // Predicate returns string | null, not boolean
  const result = findLast(array, obj => obj.name);
  expect(result).toEqual({name: 'b'});
});
