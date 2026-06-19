import {expect, test} from 'vitest';
import {
  mapAllEntries,
  mapEntries,
  mapValues,
  safeAssign,
  safeSet,
} from './objects.ts';

// Use JSON.stringify in expectations to preserve / verify key order.
const stringify = (o: unknown) => JSON.stringify(o, null, 2);

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

test('safeAssign copies own enumerable string props and returns target', () => {
  const target = {a: 1};
  const result = safeAssign(target, {b: 2, c: 3});
  // Mutates and returns the same target reference.
  expect(result).toBe(target);
  expect(result).toEqual({a: 1, b: 2, c: 3});
});

test('safeAssign overwrites existing keys', () => {
  expect(safeAssign({a: 1, b: 2}, {b: 3})).toEqual({a: 1, b: 3});
});

test('safeAssign applies sources left-to-right, last write wins', () => {
  expect(safeAssign({}, {a: 1}, {a: 2, b: 3})).toEqual({a: 2, b: 3});
});

test('safeAssign only copies own enumerable props, not inherited ones', () => {
  const source = Object.create({inherited: 'nope'});
  source.own = 'yep';
  expect(safeAssign({}, source)).toEqual({own: 'yep'});
});

test('safeAssign copies symbol keys (like Object.assign)', () => {
  const sym = Symbol('s');
  const result = safeAssign({}, {[sym]: 1, a: 2});
  expect(result[sym]).toBe(1);
  expect(result.a).toBe(2);
});

test('safeAssign skips non-enumerable own props (like Object.assign)', () => {
  const sym = Symbol('s');
  const source = {};
  Object.defineProperty(source, 'hidden', {value: 1, enumerable: false});
  Object.defineProperty(source, sym, {value: 2, enumerable: false});
  Object.defineProperty(source, 'visible', {value: 3, enumerable: true});

  const result = safeAssign({}, source) as Record<PropertyKey, unknown>;
  expect(result).toEqual({visible: 3});
  expect(result.hidden).toBeUndefined();
  expect(result[sym]).toBeUndefined();
});

test('safeAssign creates writable/enumerable/configurable data props', () => {
  const result = safeAssign({}, {a: 1});
  expect(Object.getOwnPropertyDescriptor(result, 'a')).toEqual({
    value: 1,
    writable: true,
    enumerable: true,
    configurable: true,
  });
});

test('safeAssign with a __proto__ source key sets an own property, not the prototype', () => {
  // An object literal `{__proto__: ...}` sets the prototype, so use JSON.parse
  // to get a source whose OWN enumerable key is literally "__proto__".
  const malicious = JSON.parse('{"__proto__": {"polluted": true}}');
  expect(Object.keys(malicious)).toEqual(['__proto__']);

  const target: Record<string, unknown> = {};
  safeAssign(target, malicious);

  // The key is set as a normal own property...
  expect(Object.hasOwn(target, '__proto__')).toBe(true);
  expect(Object.getOwnPropertyDescriptor(target, '__proto__')).toEqual({
    value: malicious.__proto__,
    writable: true,
    enumerable: true,
    configurable: true,
  });
  expect(Object.keys(target)).toEqual(['__proto__']);
  // ...and the prototype is left untouched (the object is not reparented).
  expect(Object.getPrototypeOf(target)).toBe(Object.prototype);
  // Global Object.prototype is never polluted.
  expect(({} as Record<string, unknown>).polluted).toBeUndefined();
});

test('safeSet sets a property and returns the target', () => {
  const target: Record<string, unknown> = {a: 1};
  const result = safeSet(target, 'b', 2);
  expect(result).toBe(target);
  expect(result).toEqual({a: 1, b: 2});
});

test('safeSet overwrites an existing key', () => {
  expect(safeSet({a: 1}, 'a', 2)).toEqual({a: 2});
});

test('safeSet supports symbol keys', () => {
  const sym = Symbol('s');
  const result = safeSet({}, sym, 1) as Record<PropertyKey, unknown>;
  expect(result[sym]).toBe(1);
});

test('safeSet creates a writable/enumerable/configurable data prop', () => {
  expect(Object.getOwnPropertyDescriptor(safeSet({}, 'a', 1), 'a')).toEqual({
    value: 1,
    writable: true,
    enumerable: true,
    configurable: true,
  });
});

test('safeSet with a __proto__ key sets an own property, not the prototype', () => {
  const target: Record<string, unknown> = {};
  const proto = {polluted: true};
  safeSet(target, '__proto__', proto);

  // Set as a normal own property, not as the prototype.
  expect(Object.hasOwn(target, '__proto__')).toBe(true);
  expect(target['__proto__']).toBe(proto);
  expect(Object.getPrototypeOf(target)).toBe(Object.prototype);
  // Global Object.prototype is never polluted.
  expect(({} as Record<string, unknown>).polluted).toBeUndefined();
});
