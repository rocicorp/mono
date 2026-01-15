import {describe, expect, expectTypeOf, test} from 'vitest';
import {getValueAtPath, iterateLeaves} from './object-traversal.ts';

test.each([
  {name: 'simple path', obj: {a: 1}, path: 'a', sep: '.', expected: 1},
  {
    name: 'nested path',
    obj: {a: {b: {c: 42}}},
    path: 'a.b.c',
    sep: '.',
    expected: 42,
  },
  {
    name: 'non-existent path',
    obj: {a: 1},
    path: 'b',
    sep: '.',
    expected: undefined,
  },
  {
    name: 'non-existent nested path',
    obj: {a: {b: 1}},
    path: 'a.c.d',
    sep: '.',
    expected: undefined,
  },
  {
    name: 'object at path',
    obj: {a: {b: {c: 1}}},
    path: 'a.b',
    sep: '.',
    expected: {c: 1},
  },
  {
    name: 'array at path',
    obj: {a: [1, 2, 3]},
    path: 'a',
    sep: '.',
    expected: [1, 2, 3],
  },
  {
    name: 'traversing through non-object',
    obj: {a: 42},
    path: 'a.b',
    sep: '.',
    expected: undefined,
  },
  {
    name: 'traversing through null',
    obj: {a: null},
    path: 'a.b',
    sep: '.',
    expected: undefined,
  },
  {
    name: 'custom separator',
    obj: {a: {b: {c: 'value'}}},
    path: 'a/b/c',
    sep: '/',
    expected: 'value',
  },
  {name: 'empty path', obj: {a: 1}, path: '', sep: '.', expected: undefined},
  {
    name: 'array index access',
    obj: {a: ['first', 'second']},
    path: 'a.0',
    sep: '.',
    expected: 'first',
  },
] as const)('$name', ({obj, path, sep, expected}) => {
  expect(getValueAtPath(obj, path, sep)).toEqual(expected);
});

test('regex separator', () => {
  const obj = {a: {b: {c: 'value'}}};
  expect(getValueAtPath(obj, 'a.b/c', /[./]/)).toBe('value');
});

// Type tests
test('type inference', () => {
  const obj = {a: {b: {c: 'hello'}}} as const;

  // Simple path returns correct type
  const a = getValueAtPath(obj, 'a', '.');
  expectTypeOf(a).toEqualTypeOf<{readonly b: {readonly c: 'hello'}}>();
  expect(a).toEqual({b: {c: 'hello'}});

  // Nested path returns correct type
  const c = getValueAtPath(obj, 'a.b.c', '.');
  expectTypeOf(c).toEqualTypeOf<'hello'>();
  expect(c).toBe('hello');

  // Non-existent path returns undefined
  const missing = getValueAtPath(obj, 'x', '.');
  expectTypeOf(missing).toEqualTypeOf<undefined>();
  expect(missing).toBe(undefined);

  // Custom separator works with types
  const custom = getValueAtPath(obj, 'a/b/c', '/');
  expectTypeOf(custom).toEqualTypeOf<'hello'>();
  expect(custom).toBe('hello');

  // Partial path returns nested object type
  const partial = getValueAtPath(obj, 'a.b', '.');
  expectTypeOf(partial).toEqualTypeOf<{readonly c: 'hello'}>();
  expect(partial).toEqual({c: 'hello'});
});

test('type inference with complex objects', () => {
  const obj = {
    users: {
      admin: {name: 'Alice', role: 'admin' as const},
      guest: {name: 'Bob', role: 'guest' as const},
    },
  } as const;

  const adminName = getValueAtPath(obj, 'users.admin.name', '.');
  expectTypeOf(adminName).toEqualTypeOf<'Alice'>();
  expect(adminName).toBe('Alice');

  const adminRole = getValueAtPath(obj, 'users.admin.role', '.');
  expectTypeOf(adminRole).toEqualTypeOf<'admin'>();
  expect(adminRole).toBe('admin');

  const users = getValueAtPath(obj, 'users', '.');
  expectTypeOf(users).toEqualTypeOf<{
    readonly admin: {readonly name: 'Alice'; readonly role: 'admin'};
    readonly guest: {readonly name: 'Bob'; readonly role: 'guest'};
  }>();
  expect(users).toEqual({
    admin: {name: 'Alice', role: 'admin'},
    guest: {name: 'Bob', role: 'guest'},
  });
});

const leafTag = Symbol('leaf');

type Leaf = {
  [leafTag]: true;
  name: string;
};

function createLeaf(name: string): Leaf {
  return {[leafTag]: true, name};
}

function isLeaf(value: unknown): value is Leaf {
  return (
    typeof value === 'object' &&
    value !== null &&
    leafTag in value &&
    (value as Leaf)[leafTag] === true
  );
}

describe('iterateLeaves', () => {
  test('yields all leaves from flat object', () => {
    const a = createLeaf('a');
    const b = createLeaf('b');
    const c = createLeaf('c');
    const obj = {a, b, c};

    const result = Array.from(iterateLeaves(obj, isLeaf));

    expect(result).toHaveLength(3);
    expect(result[0]).toBe(a);
    expect(result[1]).toBe(b);
    expect(result[2]).toBe(c);
  });

  test('yields all leaves from nested object', () => {
    const a = createLeaf('a');
    const b = createLeaf('b');
    const c = createLeaf('c');
    const obj = {
      group1: {a, b},
      group2: {c},
    };

    const result = Array.from(iterateLeaves(obj, isLeaf));

    expect(result).toHaveLength(3);
    expect(result[0]).toBe(a);
    expect(result[1]).toBe(b);
    expect(result[2]).toBe(c);
  });

  test('yields all leaves from deeply nested object', () => {
    const a = createLeaf('a');
    const b = createLeaf('b');
    const c = createLeaf('c');
    const d = createLeaf('d');
    const obj = {
      level1: {
        level2a: {a, b},
        level2b: {c},
      },
      other: {d},
    };

    const result = Array.from(iterateLeaves(obj, isLeaf));

    expect(result).toHaveLength(4);
    expect(result[0]).toBe(a);
    expect(result[1]).toBe(b);
    expect(result[2]).toBe(c);
    expect(result[3]).toBe(d);
  });

  test('yields nothing from empty object', () => {
    const result = Array.from(iterateLeaves({}, isLeaf));

    expect(result).toHaveLength(0);
  });
});
