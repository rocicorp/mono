import {assert, expect} from '@esm-bundle/chai';
import {assertJSONValue, deepClone, deepEqual, getSizeOfValue} from './json';
import type {JSONValue, ReadonlyJSONValue} from './json';

const {fail} = assert;

test('JSON deep equal', () => {
  const t = (
    a: JSONValue | undefined,
    b: JSONValue | undefined,
    expected = true,
  ) => {
    const res = deepEqual(a, b);
    if (res !== expected) {
      fail(
        JSON.stringify(a) + (expected ? ' === ' : ' !== ') + JSON.stringify(b),
      );
    }
  };

  const oneLevelOfData = [
    0,
    1,
    2,
    3,
    456789,
    true,
    false,
    null,
    '',
    'a',
    'b',
    'cdefefsfsafasdadsaas',
    [],
    {},
    {x: 4, y: 5, z: 6},
    [7, 8, 9],
  ] as const;

  const testData = [
    ...oneLevelOfData,
    [...oneLevelOfData],
    Object.fromEntries(oneLevelOfData.map(v => [JSON.stringify(v), v])),
  ];

  for (let i = 0; i < testData.length; i++) {
    for (let j = 0; j < testData.length; j++) {
      const a = testData[i];
      // "clone" to ensure we do not end up with a and b being the same object.
      const b = JSON.parse(JSON.stringify(testData[j]));
      t(a, b, i === j);
    }
  }

  t({a: 1, b: 2}, {b: 2, a: 1});
});

test('deepClone', () => {
  const t = (v: ReadonlyJSONValue) => {
    expect(deepClone(v)).to.deep.equal(v);
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
  expect(() => deepClone(cyclicObject))
    .to.throw(Error)
    .with.property('message', 'Cyclic object');

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const cyclicArray: any = {a: 42, cycle: [null]};
  cyclicArray.cycle[0] = cyclicArray;
  expect(() => deepClone(cyclicArray))
    .to.throw(Error)
    .with.property('message', 'Cyclic object');

  const sym = Symbol();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect(() => deepClone(sym as any))
    .to.throw(Error)
    .with.property('message', 'Invalid type: symbol');
});

test('deepClone - reuse references', () => {
  const t = (v: ReadonlyJSONValue) => expect(deepClone(v)).to.deep.equal(v);
  const arr: number[] = [0, 1];

  t({a: arr, b: arr});
  t(['a', [arr, arr]]);
  t(['a', arr, {a: arr}]);
  t(['a', arr, {a: [arr]}]);
});

test('getSizeOfValue', () => {
  expect(getSizeOfValue(null)).to.equal(1);
  expect(getSizeOfValue(true)).to.equal(1);
  expect(getSizeOfValue(false)).to.equal(1);

  expect(getSizeOfValue('')).to.equal(5);
  expect(getSizeOfValue('abc')).to.equal(8);

  expect(getSizeOfValue(0)).to.equal(5);
  expect(getSizeOfValue(42)).to.equal(5);
  expect(getSizeOfValue(-42)).to.equal(5);

  expect(getSizeOfValue(2 ** 7 - 1)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 7 - 1))).to.equal(5);
  expect(getSizeOfValue(2 ** 7)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 7))).to.equal(5);

  expect(getSizeOfValue(2 ** 14 - 1)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 14 - 1))).to.equal(5);
  expect(getSizeOfValue(2 ** 14)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 14))).to.equal(5);

  expect(getSizeOfValue(2 ** 21 - 1)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 21 - 1))).to.equal(5);
  expect(getSizeOfValue(2 ** 21)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 21))).to.equal(5);

  expect(getSizeOfValue(2 ** 28 - 1)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 28 - 1))).to.equal(5);
  expect(getSizeOfValue(2 ** 28)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 28))).to.equal(5);

  expect(getSizeOfValue(2 ** 31 - 1)).to.equal(6);
  expect(getSizeOfValue(-(2 ** 31))).to.equal(6);
  expect(getSizeOfValue(2 ** 31)).to.equal(9); // not smi
  expect(getSizeOfValue(-(2 ** 31) - 1)).to.equal(9); // not smi

  expect(getSizeOfValue(0.1)).to.equal(9);

  expect(getSizeOfValue([])).to.equal(1 + 5);
  expect(getSizeOfValue([0])).to.equal(6 + 5);
  expect(getSizeOfValue(['abc'])).to.equal(1 + 4 + 8 + 1);
  expect(getSizeOfValue([0, 1, 2])).to.equal(1 + 4 + 3 * 5 + 1);
  expect(getSizeOfValue([null, true, false])).to.equal(1 + 4 + 3 * 1 + 1);

  expect(getSizeOfValue({})).to.equal(1 + 4 + 1);
  expect(getSizeOfValue({abc: 'def'})).to.equal(1 + 4 + 8 + 8 + 1);
});

test('assertJSONValue', () => {
  assertJSONValue(null);
  assertJSONValue(true);
  assertJSONValue(false);
  assertJSONValue(1);
  assertJSONValue(123.456);
  assertJSONValue('');
  assertJSONValue('abc');
  assertJSONValue([]);
  assertJSONValue([1, 2, 3]);
  assertJSONValue({});
  assertJSONValue({a: 1, b: 2});
  assertJSONValue({a: 1, b: 2, c: [3, 4, 5]});

  expect(() => assertJSONValue(Symbol())).to.throw(Error);
  expect(() => assertJSONValue(() => 0)).to.throw(Error);
  expect(() => assertJSONValue(undefined)).to.throw(Error);
  expect(() => assertJSONValue(BigInt(123))).to.throw(Error);

  // Cycle
  const o = {x: {}};
  o.x = o;
  expect(() => assertJSONValue(o)).to.throw(Error);
});
