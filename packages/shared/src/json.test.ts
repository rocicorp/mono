import {expect, test} from 'vitest';
import {
  type JSONValue,
  assertJSONValue,
  deepEqual,
  isJSONValue,
} from './json.js';

test('JSON deep equal', () => {
  const t = (
    a: JSONValue | undefined,
    b: JSONValue | undefined,
    expected = true,
  ) => {
    const res = deepEqual(a, b);
    const res2 = deepEqual(b, a);
    expect(res).toBe(res2);
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

  t({a: undefined}, {a: undefined});
  t({a: 1}, Object.create({a: 1}), false);
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
  assertJSONValue({a: 1, b: undefined});
  assertJSONValue(
    Object.create({b: Symbol()}, Object.getOwnPropertyDescriptors({a: 1})),
  );

  expect(() => assertJSONValue(Symbol())).toThrow(Error);
  expect(() => assertJSONValue(() => 0)).toThrow(Error);
  expect(() => assertJSONValue(undefined)).toThrow(Error);
  expect(() => assertJSONValue(BigInt(123))).toThrow(Error);

  // Cycle
  const o = {x: {}};
  o.x = o;
  expect(() => assertJSONValue(o)).toThrow(Error);
});

test('isJSONValue', () => {
  const t = (v: unknown, expectedPath?: (string | number)[]) => {
    if (expectedPath) {
      const path: (string | number)[] = [];
      expect(isJSONValue(v, path)).toBe(false);
      expect(path).toEqual(expectedPath);
    } else {
      expect(isJSONValue(v, [])).toBe(true);
    }
  };

  t(null);
  t(true);
  t(false);
  t(1);
  t(123.456);
  t('');
  t('abc');
  t([]);
  t([1, 2, 3]);
  t({});
  t({a: 1, b: 2});
  t({a: 1, b: 2, c: [3, 4, 5]});
  t({x: undefined});
  t(Object.create({b: Symbol()}, Object.getOwnPropertyDescriptors({a: 1})));

  t(Symbol(), []);
  t(() => 0, []);
  t(undefined, []);
  t(123n, []);
  t([undefined], [0]);
  t({x: [undefined]}, ['x', 0]);
});
