import {expect, test} from 'vitest';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {
  deepFreeze,
  deepFreezeAllowUndefined,
  isDeepFrozen,
} from './frozen-json.ts';

test('deepFreeze', () => {
  expect(deepFreeze(null)).toBe(null);
  expect(deepFreeze(true)).toBe(true);
  expect(deepFreeze(false)).toBe(false);
  expect(deepFreeze(1)).toBe(1);
  expect(deepFreeze(123.456)).toBe(123.456);
  expect(deepFreeze('')).toBe('');
  expect(deepFreeze('abc')).toBe('abc');

  const expectSameObject = (v: ReadonlyJSONValue) => {
    expect(deepFreeze(v)).toBe(v);
  };

  const expectFrozen = (v: ReadonlyJSONValue) => {
    expectSameObject(v);
    expect(v).frozen;
  };

  expectFrozen([]);
  expectFrozen([1, 2, 3]);
  expectFrozen({});
  expectFrozen({a: 1, b: 2});
  expectFrozen({a: 1, b: 2, c: [3, 4, 5]});

  {
    const o = [0, 1, {a: 2, b: 3, c: [4, 5, 6]}] as const;
    const o2 = deepFreeze(o);
    expect(o2).toBe(o);
    expect(o2).frozen;
    expect(o[2]).frozen;
    expect(o[2].c).frozen;
  }

  {
    const o = {a: undefined};
    const o2 = deepFreeze(o);
    expect(o2).toBe(o);
    expect(o2).frozen;
  }

  expectFrozen({a: undefined});
});

test('isDeepFrozen', () => {
  expect(isDeepFrozen(null, [])).toBe(true);
  expect(isDeepFrozen(true, [])).toBe(true);
  expect(isDeepFrozen(1, [])).toBe(true);
  expect(isDeepFrozen('abc', [])).toBe(true);

  expect(isDeepFrozen([], [])).toBe(false);
  expect(isDeepFrozen([1, 2, 3], [])).toBe(false);
  expect(isDeepFrozen({}, [])).toBe(false);
  expect(isDeepFrozen({a: 1, b: 2}, [])).toBe(false);
  expect(isDeepFrozen({a: 1, b: 2, c: [3, 4, 5]}, [])).toBe(false);

  const o = [0, 1, {a: 2, b: 3, c: [4, 5, 6]}] as const;
  expect(isDeepFrozen(o, [])).toBe(false);
  expect(isDeepFrozen(o[2], [])).toBe(false);
  expect(isDeepFrozen(o[2].c, [])).toBe(false);
  deepFreeze(o);
  expect(isDeepFrozen(o, [])).toBe(true);
  expect(Object.isFrozen(o)).toBe(true);
  expect(isDeepFrozen(o[2], [])).toBe(true);
  expect(Object.isFrozen(o[2])).toBe(true);
  expect(isDeepFrozen(o[2].c, [])).toBe(true);
  expect(Object.isFrozen(o[2].c)).toBe(true);

  {
    const o = [0, 1, {a: 2, b: 3, c: [4, 5, 6]}] as const;
    expect(isDeepFrozen(o, [])).toBe(false);
    expect(isDeepFrozen(o[2], [])).toBe(false);
    expect(isDeepFrozen(o[2].c, [])).toBe(false);
    Object.freeze(o);
    Object.freeze(o[2]);
    expect(isDeepFrozen(o, [])).toBe(false);
    expect(Object.isFrozen(o)).toBe(true);
    expect(isDeepFrozen(o[2], [])).toBe(false);
    expect(Object.isFrozen(o[2])).toBe(true);
    expect(isDeepFrozen(o[2].c, [])).toBe(false);
    expect(Object.isFrozen(o[2].c)).toBe(false);

    Object.freeze(o[2].c);
    expect(isDeepFrozen(o, [])).toBe(true);
    expect(Object.isFrozen(o)).toBe(true);
    expect(isDeepFrozen(o[2], [])).toBe(true);
    expect(Object.isFrozen(o[2])).toBe(true);
    expect(isDeepFrozen(o[2].c, [])).toBe(true);
    expect(Object.isFrozen(o[2].c)).toBe(true);
  }

  {
    const o = {a: undefined};
    expect(isDeepFrozen(o, [])).toBe(false);
    Object.freeze(o);
    expect(isDeepFrozen(o, [])).toBe(true);
  }
});

test('deepFreeze with undefined throws', () => {
  // @ts-expect-error undefined is not allowed
  expect(() => deepFreeze(undefined)).toThrow(TypeError);

  // @ts-expect-error undefined is not allowed
  expect(() => deepFreeze([undefined])).toThrow(TypeError);

  // @ts-expect-error undefined is not allowed
  // oxlint-disable-next-line no-sparse-arrays
  expect(() => deepFreeze([1, , 2])).toThrow(TypeError);
});

test('deepFreezeAllowUndefined', () => {
  expect(deepFreezeAllowUndefined(undefined)).toBe(undefined);

  // Holes/undefined array elements are still not allowed.

  // @ts-expect-error undefined is not allowed
  expect(() => deepFreezeAllowUndefined([undefined])).toThrow(TypeError);

  // @ts-expect-error undefined is not allowed
  // oxlint-disable-next-line no-sparse-arrays
  expect(() => deepFreezeAllowUndefined([1, , 2])).toThrow(TypeError);
});
