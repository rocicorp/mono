import {expect, test} from 'vitest';
import {deepClone, deepCloneWithInstances} from './deep-clone.ts';
import type {JSONValue, ReadonlyJSONValue} from './json.ts';

test('deepCloneWithInstances - clones containers but preserves instances', () => {
  const date = new Date(1000);
  const input = {a: 1, nested: {when: date}, list: [date, {when: date}]};
  const out = deepCloneWithInstances(input);

  // Containers are fresh copies.
  expect(out).not.toBe(input);
  expect(out.nested).not.toBe(input.nested);
  expect(out.list).not.toBe(input.list);
  // Instances are preserved by reference (not turned into {}).
  expect(out.nested.when).toBe(date);
  expect(out.list[0]).toBe(date);
  expect((out.list[1] as {when: Date}).when).toBe(date);
});

test('deepCloneWithInstances - matches deepClone for plain JSON', () => {
  const input = {a: 1, b: [1, 2, {c: 'x'}], d: null};
  expect(deepCloneWithInstances(input)).toEqual(deepClone(input));
});

test('deepClone', () => {
  const t = (v: ReadonlyJSONValue) => {
    expect(deepClone(v)).toEqual(v);
  };

  t(null);
  t(1);
  t(1.2);
  t(0);
  t(-3412);
  t(1e20);
  t('');
  t('hi');
  t(true);
  t(false);
  t([]);
  t({});

  t({a: 42});
  t({a: 42, b: null});
  t({a: 42, b: 0});
  t({a: 42, b: true, c: false});
  t({a: 42, b: [1, 2, 3]});
  t([1, {}, 2]);

  const cyclicObject: JSONValue = {a: 42, cycle: null};
  cyclicObject.cycle = cyclicObject;
  expect(() => deepClone(cyclicObject)).toThrow('Cyclic object');

  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  const cyclicArray: any = {a: 42, cycle: [null]};
  cyclicArray.cycle[0] = cyclicArray;
  expect(() => deepClone(cyclicArray)).toThrow('Cyclic object');

  const sym = Symbol();
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  expect(() => deepClone(sym as any)).toThrow('Invalid type: symbol');
});

test('deepClone - reuse references', () => {
  const t = (v: ReadonlyJSONValue) => expect(deepClone(v)).toEqual(v);
  const arr: number[] = [0, 1];

  t({a: arr, b: arr});
  t(['a', [arr, arr]]);
  t(['a', arr, {a: arr}]);
  t(['a', arr, {a: [arr]}]);
});
