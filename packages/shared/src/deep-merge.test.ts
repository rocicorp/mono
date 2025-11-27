import {expect, expectTypeOf, test} from 'vitest';
import {deepMerge, type DeepMerge} from './deep-merge.ts';

test('deepMerge shallow properties', () => {
  const a = {x: 1, y: 2};
  const b = {y: 3, z: 4};
  const result = deepMerge(a, b);
  expect(result).toEqual({x: 1, y: 3, z: 4});
});

test('deepMerge nested objects', () => {
  const a = {
    user: {name: 'Alice', age: 30},
    settings: {theme: 'dark'},
  };
  const b = {
    user: {age: 31, email: 'alice@example.com'},
  };
  const result = deepMerge(a, b);
  expect(result).toEqual({
    user: {name: 'Alice', age: 31, email: 'alice@example.com'},
    settings: {theme: 'dark'},
  });
});

test('deepMerge does not mutate inputs', () => {
  const a = {nested: {value: 1}};
  const b = {nested: {other: 2}};
  deepMerge(a, b);
  expect(a).toEqual({nested: {value: 1}});
  expect(b).toEqual({nested: {other: 2}});
});

test('deepMerge arrays are merged by index', () => {
  const a = {arr: [1, 2, 3]};
  const b = {arr: [4, 5]};
  const result = deepMerge(a, b);
  // Arrays are treated as objects with numeric keys, so indices are merged
  // This results in a plain object, not an array
  expect(result).toEqual({arr: {'0': 4, '1': 5, '2': 3}});
});

test('deepMerge arrays can be replaced with custom isLeaf', () => {
  const a = {arr: [1, 2, 3]};
  const b = {arr: [4, 5]};
  const isLeaf = (v: unknown) =>
    typeof v !== 'object' || v === null || Array.isArray(v);
  const result = deepMerge(a, b, isLeaf);
  expect(result).toEqual({arr: [4, 5]});
});

test('deepMerge handles empty objects', () => {
  expect(deepMerge({}, {a: 1})).toEqual({a: 1});
  expect(deepMerge({a: 1}, {})).toEqual({a: 1});
  expect(deepMerge({}, {})).toEqual({});
});

test('deepMerge deeply nested', () => {
  const a = {a: {b: {c: {d: 1}}}};
  const b = {a: {b: {c: {e: 2}}}};
  const result = deepMerge(a, b);
  expect(result).toEqual({a: {b: {c: {d: 1, e: 2}}}});
});

test('deepMerge with custom isLeaf predicate', () => {
  const leafTag = Symbol('leaf');

  // isLeaf returns true for primitives (default behavior) or objects with leafTag
  const isLeaf = (v: unknown) =>
    typeof v !== 'object' || v === null || leafTag in (v as object);

  const a = {
    normal: {x: {v: 1}, y: {v: 2}},
    special: {[leafTag]: true, x: 1},
  };
  const b = {
    normal: {y: {v: 3}, z: {v: 4}},
    special: {[leafTag]: true, y: 2},
  };

  const result = deepMerge(a, b, isLeaf);

  // normal is merged because it's not a leaf
  expect(result.normal).toEqual({x: {v: 1}, y: {v: 3}, z: {v: 4}});
  // special is replaced because isLeaf returns true
  expect(result.special).toEqual({[leafTag]: true, y: 2});
});

test('DeepMerge type: merges disjoint properties', () => {
  type A = {x: number};
  type B = {y: string};
  type Result = DeepMerge<A, B>;

  expectTypeOf<Result>().toEqualTypeOf<{x: number; y: string}>();
});

test('DeepMerge type: b overrides a for same keys', () => {
  type A = {x: number; y: number};
  type B = {y: string};
  type Result = DeepMerge<A, B>;

  expectTypeOf<Result>().toEqualTypeOf<{x: number; y: string}>();
});

test('DeepMerge type: deeply merges nested objects', () => {
  type A = {user: {name: string; age: number}};
  type B = {user: {age: number; email: string}};
  type Result = DeepMerge<A, B>;

  expectTypeOf<Result>().toEqualTypeOf<{
    user: {name: string; age: number; email: string};
  }>();
});

test('DeepMerge type: arrays are replaced not merged', () => {
  type A = {arr: number[]};
  type B = {arr: string[]};
  type Result = DeepMerge<A, B>;

  // Arrays are not plain objects at the type level, so B wins
  expectTypeOf<Result>().toEqualTypeOf<{arr: string[]}>();
});

test('DeepMerge type: functions are replaced not merged', () => {
  type A = {fn: () => number};
  type B = {fn: () => string};
  type Result = DeepMerge<A, B>;

  expectTypeOf<Result>().toEqualTypeOf<{fn: () => string}>();
});

test('deepMerge runtime result matches type', () => {
  const a = {x: 1, nested: {a: 'hello'}};
  const b = {y: 2, nested: {b: 'world'}};
  const result = deepMerge(a, b);

  expectTypeOf(result).toEqualTypeOf<{
    x: number;
    y: number;
    nested: {a: string; b: string};
  }>();

  // Runtime check
  expect(result.x).toBe(1);
  expect(result.y).toBe(2);
  expect(result.nested.a).toBe('hello');
  expect(result.nested.b).toBe('world');
});
