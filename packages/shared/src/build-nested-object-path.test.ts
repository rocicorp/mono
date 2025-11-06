import {describe, expect, expectTypeOf, test} from 'vitest';
import {
  buildNestedObjectPath,
  type BuildNested,
} from './build-nested-object-path.ts';

describe('buildNestedObjectPath', () => {
  test('creates single-level nested object', () => {
    const target = {};
    const result = buildNestedObjectPath(target, 'users', '.', 'value');

    expect(result).toEqual({users: 'value'});
    expect(result).toBe(target); // Same reference
  });

  test('creates two-level nested object', () => {
    const target = {};
    const result = buildNestedObjectPath(
      target,
      'users.create',
      '.',
      'createFn',
    );

    expect(result).toEqual({users: {create: 'createFn'}});
  });

  test('creates three-level nested object', () => {
    const target = {};
    const result = buildNestedObjectPath(
      target,
      'users.profile.update',
      '.',
      'updateFn',
    );

    expect(result).toEqual({users: {profile: {update: 'updateFn'}}});
  });

  test('progressively builds nested structure', () => {
    const obj1 = {};
    const obj2 = buildNestedObjectPath(obj1, 'users.create', '.', 'createFn');
    const obj3 = buildNestedObjectPath(obj2, 'users.update', '.', 'updateFn');
    const obj4 = buildNestedObjectPath(
      obj3,
      'posts.create',
      '.',
      'postCreateFn',
    );

    expect(obj4).toEqual({
      users: {
        create: 'createFn',
        update: 'updateFn',
      },
      posts: {
        create: 'postCreateFn',
      },
    });
    // All should be the same reference
    expect(obj1).toBe(obj2);
    expect(obj2).toBe(obj3);
    expect(obj3).toBe(obj4);
  });

  test('handles custom separators', () => {
    const target = {};
    const result = buildNestedObjectPath(
      target,
      'users/profile/update',
      '/',
      'updateFn',
    );

    expect(result).toEqual({users: {profile: {update: 'updateFn'}}});
  });

  test('preserves existing nested structure', () => {
    const target = {
      users: {
        create: 'createFn',
      },
    };

    buildNestedObjectPath(target, 'users.update', '.', 'updateFn');

    expect(target).toEqual({
      users: {
        create: 'createFn',
        update: 'updateFn',
      },
    });
  });

  test('overwrites leaf values', () => {
    const target = {};
    buildNestedObjectPath(target, 'users.create', '.', 'createFn1');
    buildNestedObjectPath(target, 'users.create', '.', 'createFn2');

    expect(target).toEqual({
      users: {
        create: 'createFn2',
      },
    });
  });

  test('works with object values', () => {
    const target = {};
    const value = {foo: 'bar', nested: {prop: 42}};
    const result = buildNestedObjectPath(target, 'config.settings', '.', value);

    expect(result).toEqual({config: {settings: value}});
    expect((result as {config: {settings: typeof value}}).config.settings).toBe(
      value,
    );
  });

  test('handles deep nesting', () => {
    const target = {};
    const result = buildNestedObjectPath(
      target,
      'a.b.c.d.e.f',
      '.',
      'deepValue',
    );

    expect(result).toEqual({
      a: {
        b: {
          c: {
            d: {
              e: {
                f: 'deepValue',
              },
            },
          },
        },
      },
    });
  });
});

describe('buildNestedObjectPath - type tests', () => {
  test('single level path creates correct type', () => {
    const target = {};
    const result = buildNestedObjectPath(target, 'users', '.', 'value');

    // Type inference should create nested structure
    type Expected = {users: string};
    expectTypeOf(result).toEqualTypeOf<Expected>();
  });

  test('two level path creates correct nested type', () => {
    const target = {};
    const result = buildNestedObjectPath(
      target,
      'users.create',
      '.',
      'createFn',
    );

    type Expected = {users: {create: string}};
    expectTypeOf(result).toEqualTypeOf<Expected>();
  });

  test('three level path creates correct nested type', () => {
    const target = {};
    const result = buildNestedObjectPath(
      target,
      'users.profile.update',
      '.',
      'updateFn',
    );

    type Expected = {users: {profile: {update: string}}};
    expectTypeOf(result).toEqualTypeOf<Expected>();
  });

  test('progressive building refines types correctly', () => {
    const obj1 = {};
    const obj2 = buildNestedObjectPath(obj1, 'users.create', '.', 'createFn');
    const obj3 = buildNestedObjectPath(obj2, 'users.update', '.', 'updateFn');

    // After first call
    type Expected2 = {users: {create: string}};
    expectTypeOf(obj2).toEqualTypeOf<Expected2>();

    // After second call - should merge types
    type Expected3 = {users: {create: string; update: string}};
    expectTypeOf(obj3).toEqualTypeOf<Expected3>();
  });

  test('function values preserve function type', () => {
    const target = {};
    const fn = (x: number) => x.toString();
    const result = buildNestedObjectPath(target, 'users.create', '.', fn);

    type Expected = {users: {create: (x: number) => string}};
    expectTypeOf(result).toEqualTypeOf<Expected>();
  });

  test('custom separator works with types', () => {
    const target = {};
    const result = buildNestedObjectPath(
      target,
      'users/profile/update',
      '/',
      42,
    );

    // Runtime behavior
    expect(result).toEqual({users: {profile: {update: 42}}});

    // Type should correctly infer with custom separator
    type Expected = {users: {profile: {update: number}}};
    expectTypeOf(result).toEqualTypeOf<Expected>();
  });
});

describe('BuildNested type', () => {
  test('single level path type with dot separator', () => {
    type Result = BuildNested<'users', '.', string>;
    expectTypeOf<Result>().toEqualTypeOf<{users: string}>();
  });

  test('two level path type with dot separator', () => {
    type Result = BuildNested<'users.create', '.', string>;
    expectTypeOf<Result>().toEqualTypeOf<{users: {create: string}}>();
  });

  test('three level path type with dot separator', () => {
    type Result = BuildNested<'users.profile.update', '.', number>;
    expectTypeOf<Result>().toEqualTypeOf<{
      users: {profile: {update: number}};
    }>();
  });

  test('custom separator in type', () => {
    type Result = BuildNested<'users/profile/update', '/', boolean>;
    expectTypeOf<Result>().toEqualTypeOf<{
      users: {profile: {update: boolean}};
    }>();
  });

  test('pipe separator in type', () => {
    type Result = BuildNested<'a|b|c', '|', string>;
    expectTypeOf<Result>().toEqualTypeOf<{a: {b: {c: string}}}>();
  });

  test('preserves value type', () => {
    type Fn = (x: number) => string;
    type Result = BuildNested<'a.b.c', '.', Fn>;
    expectTypeOf<Result>().toEqualTypeOf<{a: {b: {c: Fn}}}>();
  });
});
