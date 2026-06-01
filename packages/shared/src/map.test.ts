import {expect, test} from 'vitest';
import {getESLibVersion} from './get-es-lib-version.ts';
import {getOrInsert, getOrInsertComputed} from './map.ts';

test('lib < ES2027', () => {
  // sanity check that we are not using es2027. If this starts failing we can
  // remove the polyfill and update the code to use the native version.
  expect(getESLibVersion()).toBeLessThan(2027);
});

test('getOrInsert', () => {
  const map = new Map<string, number>();
  expect(getOrInsert(map, 'a', 1)).toBe(1);
  expect(getOrInsert(map, 'a', 2)).toBe(1);
  expect(getOrInsert(map, 'b', 3)).toBe(3);
});

test('getOrInsertComputed', () => {
  const map = new Map<string, number>();
  expect(getOrInsertComputed(map, 'a', () => 1)).toBe(1);
  expect(getOrInsertComputed(map, 'a', () => 2)).toBe(1);
  expect(getOrInsertComputed(map, 'b', () => 3)).toBe(3);
});
