import {expect, test} from 'vitest';
import {
  assignProperty,
  mapAllEntries,
  mapEntries,
  mapValues,
} from './objects.ts';

// Use JSON.stringify in expectations to preserve / verify key order.
const stringify = (o: unknown) => JSON.stringify(o, null, 2);

const inputWithProtoKey = <T>(value: T): Record<string, T> => {
  const input: Record<string, T> = {};
  Object.defineProperty(input, '__proto__', {
    value,
    writable: true,
    enumerable: true,
    configurable: true,
  });
  return input;
};

test('mapValues', () => {
  const obj = {
    foo: 'bar',
    bar: 'baz',
    boo: 'doo',
  };

  expect(stringify(mapValues(obj, v => v.toUpperCase())))
    .toMatchInlineSnapshot(`
    "{
      "foo": "BAR",
      "bar": "BAZ",
      "boo": "DOO"
    }"
  `);
});

test('mapValues safely maps __proto__ as an own property', () => {
  const mapped = mapValues(inputWithProtoKey('bar'), v => v.toUpperCase());

  expect(Object.getPrototypeOf(mapped)).toBe(Object.prototype);
  expect(Object.hasOwn(mapped, '__proto__')).toBe(true);
  expect(mapped.__proto__).toBe('BAR');
  expect(Object.keys(mapped)).toEqual(['__proto__']);
});

test('mapEntries', () => {
  const obj = {
    boo: 'doo',
    foo: 'bar',
    bar: 'baz',
  };

  expect(stringify(mapEntries(obj, (k, v) => [v, k]))).toMatchInlineSnapshot(`
    "{
      "doo": "boo",
      "bar": "foo",
      "baz": "bar"
    }"
  `);
});

test('mapEntries safely maps __proto__ as an own property', () => {
  const mapped = mapEntries({foo: 'bar'}, () => ['__proto__', 'baz']);

  expect(Object.getPrototypeOf(mapped)).toBe(Object.prototype);
  expect(Object.hasOwn(mapped, '__proto__')).toBe(true);
  expect(mapped.__proto__).toBe('baz');
  expect(Object.keys(mapped)).toEqual(['__proto__']);
});

test('mapAllEntries', () => {
  const obj = {
    foo: 'bar',
    bar: 'baz',
    boo: 'doo',
  };

  const sorted = mapAllEntries(obj, e =>
    e.sort(([a], [b]) => a.localeCompare(b)),
  );
  expect(stringify(sorted)).toMatchInlineSnapshot(`
    "{
      "bar": "baz",
      "boo": "doo",
      "foo": "bar"
    }"
  `);

  const reversed = mapAllEntries(obj, e =>
    e.sort(([a], [b]) => a.localeCompare(b) * -1),
  );
  expect(stringify(reversed)).toMatchInlineSnapshot(`
    "{
      "foo": "bar",
      "boo": "doo",
      "bar": "baz"
    }"
  `);
});

test('mapAllEntries safely maps __proto__ as an own property', () => {
  const mapped = mapAllEntries({foo: 'bar'}, () => [['__proto__', 'baz']]);

  expect(Object.getPrototypeOf(mapped)).toBe(Object.prototype);
  expect(Object.hasOwn(mapped, '__proto__')).toBe(true);
  expect(mapped.__proto__).toBe('baz');
  expect(Object.keys(mapped)).toEqual(['__proto__']);
});

test('assignProperty safely assigns __proto__ as an own property', () => {
  const target: Record<string, string> = {};

  assignProperty(target, '__proto__', 'bar');

  expect(Object.getPrototypeOf(target)).toBe(Object.prototype);
  expect(Object.hasOwn(target, '__proto__')).toBe(true);
  expect(target.__proto__).toBe('bar');
});
