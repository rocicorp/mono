import {expect, test} from 'vitest';
import {getValueAtPath} from './get-value-at-path.ts';

test('returns value at simple path', () => {
  const obj = {a: 1};
  expect(getValueAtPath(obj, 'a', '.')).toBe(1);
});

test('returns value at nested path', () => {
  const obj = {a: {b: {c: 42}}};
  expect(getValueAtPath(obj, 'a.b.c', '.')).toBe(42);
});

test('returns undefined for non-existent path', () => {
  const obj = {a: 1};
  expect(getValueAtPath(obj, 'b', '.')).toBe(undefined);
});

test('returns undefined for non-existent nested path', () => {
  const obj = {a: {b: 1}};
  expect(getValueAtPath(obj, 'a.c.d', '.')).toBe(undefined);
});

test('returns value at path', () => {
  const obj = {a: {b: {c: 1}}};
  expect(getValueAtPath(obj, 'a.b', '.')).toEqual({c: 1});
});

test('returns array at path', () => {
  const obj = {a: [1, 2, 3]};
  expect(getValueAtPath(obj, 'a', '.')).toEqual([1, 2, 3]);
});

test('returns undefined when traversing through non-object', () => {
  const obj = {a: 42};
  expect(getValueAtPath(obj, 'a.b', '.')).toBe(undefined);
});

test('returns undefined when traversing through null', () => {
  const obj = {a: null};
  expect(getValueAtPath(obj, 'a.b', '.')).toBe(undefined);
});

test('works with custom separator', () => {
  const obj = {a: {b: {c: 'value'}}} as const;
  const result = getValueAtPath(obj, 'a/b/c', '/');
  result satisfies 'value';
  expect(result).toBe('value');
});

test('works with regex separator', () => {
  const obj = {a: {b: {c: 'value'}}};
  expect(getValueAtPath(obj, 'a.b/c', /[./]/)).toBe('value');
});

test('returns undefined for empty path', () => {
  const obj = {a: 1};
  expect(getValueAtPath(obj, '', '.')).toBe(undefined);
});

test('handles array index access', () => {
  const obj = {a: ['first', 'second']};
  expect(getValueAtPath(obj, 'a.0', '.')).toBe('first');
});
