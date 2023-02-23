import {assert, expect} from '@esm-bundle/chai';
import {
  assertJSONValue,
  deepEqual,
  getSizeOfValue,
  ReadonlyJSONValue,
  deepFreeze,
  isDeepFrozen,
} from './json.js';
import type {JSONValue} from './json.js';

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

test('toDeepFrozen', () => {
  expect(deepFreeze(null)).to.equal(null);
  expect(deepFreeze(true)).to.equal(true);
  expect(deepFreeze(false)).to.equal(false);
  expect(deepFreeze(1)).to.equal(1);
  expect(deepFreeze(123.456)).to.equal(123.456);
  expect(deepFreeze('')).to.equal('');
  expect(deepFreeze('abc')).to.equal('abc');

  const expectSameObject = (v: ReadonlyJSONValue) => {
    expect(deepFreeze(v)).to.equal(v);
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

  const o = [0, 1, {a: 2, b: 3, c: [4, 5, 6]}] as const;
  const o2 = deepFreeze(o);
  expect(o2).equal(o);
  expect(o2).frozen;
  expect(o[2]).frozen;
  expect(o[2].c).frozen;
});

test('isDeepFrozen', () => {
  expect(isDeepFrozen(null, [])).to.be.true;
  expect(isDeepFrozen(true, [])).to.be.true;
  expect(isDeepFrozen(1, [])).to.be.true;
  expect(isDeepFrozen('abc', [])).to.be.true;

  expect(isDeepFrozen([], [])).to.be.false;
  expect(isDeepFrozen([1, 2, 3], [])).to.be.false;
  expect(isDeepFrozen({}, [])).to.be.false;
  expect(isDeepFrozen({a: 1, b: 2}, [])).to.be.false;
  expect(isDeepFrozen({a: 1, b: 2, c: [3, 4, 5]}, [])).to.be.false;

  const o = [0, 1, {a: 2, b: 3, c: [4, 5, 6]}] as const;
  expect(isDeepFrozen(o, [])).to.be.false;
  expect(isDeepFrozen(o[2], [])).to.be.false;
  expect(isDeepFrozen(o[2].c, [])).to.be.false;
  deepFreeze(o);
  expect(isDeepFrozen(o, [])).to.be.true;
  expect(Object.isFrozen(o)).to.be.true;
  expect(isDeepFrozen(o[2], [])).to.be.true;
  expect(Object.isFrozen(o[2])).to.be.true;
  expect(isDeepFrozen(o[2].c, [])).to.be.true;
  expect(Object.isFrozen(o[2].c)).to.be.true;

  {
    const o = [0, 1, {a: 2, b: 3, c: [4, 5, 6]}] as const;
    expect(isDeepFrozen(o, [])).to.be.false;
    expect(isDeepFrozen(o[2], [])).to.be.false;
    expect(isDeepFrozen(o[2].c, [])).to.be.false;
    Object.freeze(o);
    Object.freeze(o[2]);
    expect(isDeepFrozen(o, [])).to.be.false;
    expect(Object.isFrozen(o)).to.be.true;
    expect(isDeepFrozen(o[2], [])).to.be.false;
    expect(Object.isFrozen(o[2])).to.be.true;
    expect(isDeepFrozen(o[2].c, [])).to.be.false;
    expect(Object.isFrozen(o[2].c)).to.be.false;

    Object.freeze(o[2].c);
    expect(isDeepFrozen(o, [])).to.be.true;
    expect(Object.isFrozen(o)).to.be.true;
    expect(isDeepFrozen(o[2], [])).to.be.true;
    expect(Object.isFrozen(o[2])).to.be.true;
    expect(isDeepFrozen(o[2].c, [])).to.be.true;
    expect(Object.isFrozen(o[2].c)).to.be.true;
  }
});
