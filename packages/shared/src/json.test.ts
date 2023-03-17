import {expect, test} from '@jest/globals';
import type {JSONValue} from './json.js';
import {assertJSONValue, deepEqual} from './json.js';

test('JSON deep equal', () => {
  const t = (
    a: JSONValue | undefined,
    b: JSONValue | undefined,
    expected = true,
  ) => {
    const res = deepEqual(a, b);
    if (res !== expected) {
      throw new Error(
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

  expect(() => assertJSONValue(Symbol())).toThrow(Error);
  expect(() => assertJSONValue(() => 0)).toThrow(Error);
  expect(() => assertJSONValue(undefined)).toThrow(Error);
  expect(() => assertJSONValue(BigInt(123))).toThrow(Error);

  // Cycle
  const o = {x: {}};
  o.x = o;
  expect(() => assertJSONValue(o)).toThrow(Error);
});
