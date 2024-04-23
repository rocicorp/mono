import {expect, test} from 'vitest';
import {deepClone} from './deep-clone.js';
import type {JSONValue, ReadonlyJSONValue} from './json.js';

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

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const cyclicArray: any = {a: 42, cycle: [null]};
  cyclicArray.cycle[0] = cyclicArray;
  expect(() => deepClone(cyclicArray)).toThrow('Cyclic object');

  const sym = Symbol();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
