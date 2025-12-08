import {expect, test} from 'vitest';
import {recordProxy} from './record-proxy.ts';

test('returns undefined for symbol properties', () => {
  const proxy = recordProxy({a: 1}, (source, _prop) => String(source));

  // oxlint-disable-next-line no-explicit-any
  expect((proxy as any)[Symbol.toStringTag]).toBeUndefined();
});

test('returns transformed value for existing properties', () => {
  const proxy = recordProxy({a: 1, b: 2, c: 3}, (source, _prop) =>
    String(source * 2),
  );

  expect(proxy.a).toBe('2');
  expect(proxy.b).toBe('4');
  expect(proxy.c).toBe('6');
});

test('returns undefined for non-existing properties', () => {
  const proxy = recordProxy({a: 1}, (source, _prop) => String(source));

  expect(proxy.nonExistent).toBeUndefined();
});

test('caches transformed values', () => {
  let callCount = 0;
  const proxy = recordProxy({a: 1}, (source, _prop) => {
    callCount++;
    return String(source);
  });

  expect(proxy.a).toBe('1');
  expect(callCount).toBe(1);

  expect(proxy.a).toBe('1');
  expect(callCount).toBe(1); // Should not increment, value is cached
});

test('passes property name to transform function', () => {
  const proxy = recordProxy(
    {foo: 42, bar: 99},
    (source, prop) => `${prop}:${source}`,
  );

  expect(proxy.foo).toBe('foo:42');
  expect(proxy.bar).toBe('bar:99');
});

test('getOwnPropertyDescriptor returns undefined for symbol properties', () => {
  const proxy = recordProxy({a: 1}, (source, _prop) => String(source));

  const desc = Object.getOwnPropertyDescriptor(proxy, Symbol.toStringTag);
  expect(desc).toBeUndefined();
});

test('getOwnPropertyDescriptor populates cache and returns descriptor', () => {
  let callCount = 0;
  const proxy = recordProxy({a: 1}, (source, _prop) => {
    callCount++;
    return String(source * 2);
  });

  // getOwnPropertyDescriptor should populate the cache
  const desc = Object.getOwnPropertyDescriptor(proxy, 'a');
  expect(desc).toBeDefined();
  expect(desc?.value).toBe('2');
  expect(callCount).toBe(1);

  // Subsequent access should use cached value
  expect(proxy.a).toBe('2');
  expect(callCount).toBe(1);
});

test('getOwnPropertyDescriptor returns descriptor with cached value', () => {
  const proxy = recordProxy({a: 1}, (source, _prop) => String(source * 2));

  // Access property to cache it
  expect(proxy.a).toBe('2');

  const desc = Object.getOwnPropertyDescriptor(proxy, 'a');
  expect(desc).toBeDefined();
  expect(desc?.value).toBe('2');
});

test('works with complex object transformations', () => {
  type Source = {x: number; y: number};
  type Dest = {sum: number; product: number};

  const proxy = recordProxy(
    {
      point1: {x: 2, y: 3},
      point2: {x: 4, y: 5},
    },
    (source: Source, _prop): Dest => ({
      sum: source.x + source.y,
      product: source.x * source.y,
    }),
  );

  expect(proxy.point1).toEqual({sum: 5, product: 6});
  expect(proxy.point2).toEqual({sum: 9, product: 20});
});

test('returns inherited properties without transformation and calls onMissing', () => {
  const proto = {inherited: 42};
  const target = Object.create(proto) as Record<string, number>;
  target.own = 1;
  const missingProps: string[] = [];
  const proxy = recordProxy(
    target,
    (source, _prop) => String(source),
    prop => missingProps.push(prop),
  );

  // own properties are transformed
  expect(proxy.own).toBe('1');
  expect(missingProps).toEqual([]);

  // inherited properties are returned as-is (not transformed), but onMissing is called
  expect(proxy.inherited).toBe(42);
  expect(missingProps).toEqual(['inherited']);

  // getOwnPropertyDescriptor only returns own properties
  expect(Object.getOwnPropertyDescriptor(proxy, 'own')).toBeDefined();
  expect(Object.getOwnPropertyDescriptor(proxy, 'inherited')).toBeUndefined();
});

test('onMissing callback is called for missing properties', () => {
  const proxy = recordProxy(
    {a: 1},
    (source, _prop) => String(source),
    prop => {
      throw new Error(`Property ${prop} not found`);
    },
  );

  expect(proxy.a).toBe('1');
  expect(() => proxy.nonExistent).toThrow('Property nonExistent not found');
});

test('Object.keys returns own property keys', () => {
  const proto = {inherited: 42};
  const target = Object.create(proto) as Record<string, number>;
  target.a = 1;
  target.b = 2;
  const proxy = recordProxy(target, (source, _prop) => String(source));

  expect(Object.keys(proxy)).toEqual(['a', 'b']);
});

test('for-in iterates over own and inherited properties', () => {
  const proto = {inherited: 42};
  const target = Object.create(proto) as Record<string, number>;
  target.a = 1;
  target.b = 2;
  const proxy = recordProxy(target, (source, _prop) => String(source));

  const keys: string[] = [];
  for (const key in proxy) {
    keys.push(key);
  }
  expect(keys).toEqual(['a', 'b', 'inherited']);
});
