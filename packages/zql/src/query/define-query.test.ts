import type {StandardSchemaV1} from '@standard-schema/spec';
import {describe, expect, expectTypeOf, test} from 'vitest';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {defineQuery} from './define-query.ts';
import {createBuilder} from './named.ts';
import {queryWithContext} from './query-internals.ts';

const schema = createSchema({
  tables: [
    table('foo')
      .columns({
        id: string(),
        val: number(),
      })
      .primaryKey('id'),
  ],
});

const builder = createBuilder(schema);

function makeValidator<Input, Output>(
  validate: (data: unknown) => Output,
): StandardSchemaV1<Input, Output> {
  return {
    '~standard': {
      version: 1,
      vendor: 'test',
      validate: data => {
        try {
          return {value: validate(data)};
        } catch (e) {
          return {issues: [{message: (e as Error).message}]};
        }
      },
    },
  };
}

describe('defineQuery', () => {
  test('should work without options parameter', () => {
    const query = defineQuery(
      'testNoOptions',
      ({ctx}: {ctx: string; args: undefined}) => {
        expect(ctx).toBe('noOptionsContext');
        return builder.foo.where('id', '=', 'noOptionsId');
      },
    );

    expect(query.queryName).toBe('testNoOptions');

    const result = queryWithContext(query(undefined), 'noOptionsContext');
    expect(result.ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'noOptionsId'},
      },
      orderBy: [['id', 'asc']],
    });
  });

  test('should work with empty options object (no validator)', () => {
    const query = defineQuery(
      'testEmptyOptions',
      {},
      ({ctx, args}: {ctx: string; args: number}) => {
        expect(ctx).toBe('emptyContext');
        expect(args).toBe(42);
        return builder.foo.where('val', '=', args);
      },
    );

    expect(query.queryName).toBe('testEmptyOptions');

    const result = queryWithContext(query(42), 'emptyContext');
    expect(result.ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'val'},
        op: '=',
        right: {type: 'literal', value: 42},
      },
      orderBy: [['id', 'asc']],
    });
  });

  test('should work with validator that transforms input', () => {
    const query = defineQuery(
      'testWithValidator',
      {
        validator: makeValidator<string, number>(data => {
          if (typeof data === 'string') {
            const parsed = parseInt(data, 10);
            if (isNaN(parsed)) {
              throw new Error('Invalid number');
            }
            return parsed;
          }
          throw new Error('Expected string');
        }),
      },
      ({ctx, args}: {ctx: string; args: number}) => {
        expect(ctx).toBe('validatorContext');
        expect(args).toBe(123); // Should be converted from string to number
        expect(typeof args).toBe('number');
        return builder.foo.where('val', '=', args);
      },
    );

    expect(query.queryName).toBe('testWithValidator');

    // Input is string, but should be converted to number by validator
    const result = queryWithContext(query('123'), 'validatorContext');
    expect(result.ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'val'},
        op: '=',
        right: {type: 'literal', value: 123}, // Should be the converted number
      },
      orderBy: [['id', 'asc']],
    });
  });

  test('should work with validator that returns undefined', () => {
    const query = defineQuery(
      'testUndefinedValidator',
      {
        validator: {
          '~standard': {
            version: 1,
            vendor: 'test',
            validate: (_data: unknown) => ({value: undefined}),
          },
        } as StandardSchemaV1<unknown, undefined>,
      },
      ({ctx, args}: {ctx: string; args: undefined}) => {
        expect(ctx).toBe('undefinedValidatorContext');
        expect(args).toBeUndefined();
        return builder.foo.where('val', '=', 1);
      },
    );

    const result = queryWithContext(query(), 'undefinedValidatorContext');
    expect(result.ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'val'},
        op: '=',
        right: {type: 'literal', value: 1},
      },
      orderBy: [['id', 'asc']],
    });
  });

  test('should throw error when validator fails', () => {
    const query = defineQuery(
      'testValidatorError',
      {
        validator: {
          '~standard': {
            version: 1,
            vendor: 'test',
            validate: (_data: unknown) => ({
              issues: [{path: ['args'], message: 'Invalid input'}],
            }),
          },
        } as StandardSchemaV1<unknown, string>,
      },
      ({args}: {ctx: string; args: string}) =>
        builder.foo.where('id', '=', args),
    );

    expect(() => queryWithContext(query('invalid'), 'errorContext')).toThrow(
      'Validation failed for query testValidatorError',
    );
  });

  test('should throw error when validator returns a promise', () => {
    const query = defineQuery(
      'testAsyncValidator',
      {
        validator: {
          '~standard': {
            version: 1,
            vendor: 'test',
            validate: async (data: unknown) => {
              // Simulate async validation
              await new Promise(resolve => setTimeout(resolve, 1));
              return {value: `processed-${String(data)}`};
            },
          },
        } as StandardSchemaV1<string, string>,
      },
      ({ctx, args}: {ctx: string; args: string}) => {
        expect(ctx).toBe('asyncContext');
        expect(args).toBe('processed-input');
        return builder.foo.where('id', '=', args);
      },
    );

    expect(() => queryWithContext(query('input'), 'asyncContext')).toThrow(
      'Async validators are not supported. Query name testAsyncValidator',
    );
  });

  test('should preserve query name property and work with different overloads', () => {
    const query1 = defineQuery(
      'query1',
      ({ctx}: {ctx: string; args: undefined}) => {
        expect(ctx).toBe('ctx1');
        return builder.foo;
      },
    );
    const query2 = defineQuery(
      'query2',
      {},
      ({ctx}: {ctx: string; args: undefined}) => {
        expect(ctx).toBe('ctx2');
        return builder.foo;
      },
    );
    const query3 = defineQuery(
      'query3',
      {},
      ({ctx}: {ctx: string; args: undefined}) => {
        expect(ctx).toBe('ctx3');
        return builder.foo;
      },
    );
    const query4 = defineQuery(
      'query4',
      {validator: makeValidator((x: unknown) => x as string)},
      ({ctx, args}: {ctx: string; args: string}) => {
        expect(ctx).toBe('ctx4');
        expect(args).toBe('test');
        return builder.foo;
      },
    );

    // Test that all overloads actually work
    const result1 = queryWithContext(query1(undefined), 'ctx1');
    const result2 = queryWithContext(query2(undefined), 'ctx2');
    const result3 = queryWithContext(query3(undefined), 'ctx3');
    const result4 = queryWithContext(query4('test'), 'ctx4');

    // All should return the basic table query
    [result1, result2, result3, result4].forEach(result => {
      expect(result.ast).toEqual({
        table: 'foo',
        orderBy: [['id', 'asc']],
      });
    });
  });
});

// Type Tests
describe('defineQuery types', () => {
  test('no type annotations should treat ctx as unknown and args as ReadonlyJSONValue | undefined', () => {
    // Test no options parameter
    const query1 = defineQuery('noOptionsNoAnnotations', ({ctx, args}) => {
      expectTypeOf(ctx).toEqualTypeOf<unknown>();
      expectTypeOf(args).toEqualTypeOf<ReadonlyJSONValue | undefined>();
      return builder.foo.where('val', '=', 123);
    });

    // Test empty options parameter - should behave the same
    const query2 = defineQuery(
      'emptyOptionsNoAnnotations',
      {},
      ({ctx, args}) => {
        expectTypeOf(ctx).toEqualTypeOf<unknown>();
        expectTypeOf(args).toEqualTypeOf<ReadonlyJSONValue | undefined>();
        return builder.foo.where('val', '=', 123);
      },
    );

    // Note: undefined options parameter is not allowed in 3-arg form

    // All should accept ReadonlyJSONValue types for args
    expectTypeOf(query1).toBeCallableWith(42);
    expectTypeOf(query1).toBeCallableWith('string');
    expectTypeOf(query1).toBeCallableWith(true);
    expectTypeOf(query1).toBeCallableWith(null);
    expectTypeOf(query1).toBeCallableWith(undefined);
    expectTypeOf(query1).toBeCallableWith();

    // Same for query2 (query3 with undefined options not supported in 3-arg form)
    expectTypeOf(query2).toBeCallableWith(42);
  });

  test('with type annotations should respect those types', () => {
    // Test no options parameter with type annotations
    const query1 = defineQuery(
      'noOptionsWithAnnotations',
      ({ctx, args}: {ctx: string; args: number}) => {
        expectTypeOf(ctx).toEqualTypeOf<string>();
        expectTypeOf(args).toEqualTypeOf<number>();
        return builder.foo.where('val', '=', args);
      },
    );

    // Test empty options parameter - should behave the same
    const query2 = defineQuery(
      'emptyOptionsWithAnnotations',
      {},
      ({ctx, args}: {ctx: string; args: number}) => {
        expectTypeOf(ctx).toEqualTypeOf<string>();
        expectTypeOf(args).toEqualTypeOf<number>();
        return builder.foo.where('val', '=', args);
      },
    );

    // Note: undefined options parameter not supported in 3-arg form

    // All should respect the type annotations
    expectTypeOf(query1).toBeCallableWith(42);
    expectTypeOf(query2).toBeCallableWith(42);

    // @ts-expect-error - Type 'boolean' is not assignable to type 'number'
    expectTypeOf(query2).toBeCallableWith(true);

    // @ts-expect-error - Type 'boolean' is not assignable to type 'string'
    expectTypeOf(query2).toBeCallableWith({ctx: false, args: 42});

    // Should have correct query names
    expectTypeOf(query1.queryName).toEqualTypeOf<'noOptionsWithAnnotations'>();
    expectTypeOf(
      query2.queryName,
    ).toEqualTypeOf<'emptyOptionsWithAnnotations'>();
  });

  test('validator with same input/output type', () => {
    const query = defineQuery(
      'validatorSameType',
      {
        validator: makeValidator<string, string>(data => {
          if (typeof data === 'string') {
            return data.toUpperCase();
          }
          throw new Error('Expected string');
        }),
      },
      ({ctx: _ctx, args}) => {
        expectTypeOf(args).toEqualTypeOf<string>();
        return builder.foo.where('id', '=', args);
      },
    );

    expectTypeOf(query.queryName).toEqualTypeOf<'validatorSameType'>();
    expectTypeOf(query).toBeCallableWith('hello');
  });

  test('undefined args handling', () => {
    // All three forms should behave the same when args is undefined
    const query1 = defineQuery(
      'undefinedArgs1',
      ({args}: {args: undefined}) => {
        expectTypeOf(args).toEqualTypeOf<undefined>();
        return builder.foo;
      },
    );

    const query2 = defineQuery(
      'undefinedArgs2',
      {},
      ({args}: {args: undefined}) => {
        expectTypeOf(args).toEqualTypeOf<undefined>();
        return builder.foo;
      },
    );

    const query3 = defineQuery(
      'undefinedArgs3',
      {
        validator: makeValidator<undefined, undefined>(x => x as undefined),
      },
      ({args}) => {
        expectTypeOf(args).toEqualTypeOf<undefined>();
        return builder.foo;
      },
    );

    // Note: query3 with undefined options not supported in 3-arg form

    // All should be callable with args: undefined
    expectTypeOf(query1).toBeCallableWith(undefined);
    expectTypeOf(query2).toBeCallableWith(undefined);
    expectTypeOf(query3).toBeCallableWith(undefined);

    expectTypeOf(query1).toBeCallableWith();
    expectTypeOf(query2).toBeCallableWith();
    expectTypeOf(query3).toBeCallableWith();

    // Should not accept any other types
    // @ts-expect-error - Type 'number' is not assignable to type 'undefined'
    expectTypeOf(query1).toBeCallableWith(123);
    // @ts-expect-error - Type 'string' is not assignable to type 'undefined'
    expectTypeOf(query2).toBeCallableWith('test');
    // @ts-expect-error - Type 'boolean' is not assignable to type 'undefined'
    expectTypeOf(query3).toBeCallableWith(true);
  });

  test('No validator and no type annotations should treat args as ReadonlyJSONValue | undefined', () => {
    const query = defineQuery('noValidatorNoAnnotations', ({args}) => {
      expectTypeOf(args).toEqualTypeOf<ReadonlyJSONValue | undefined>();
      return builder.foo;
    });

    // Should accept any ReadonlyJSONValue type
    expectTypeOf(query).toBeCallableWith(42);
    expectTypeOf(query).toBeCallableWith('string');
    expectTypeOf(query).toBeCallableWith(true);
    expectTypeOf(query).toBeCallableWith(null);
    expectTypeOf(query).toBeCallableWith(undefined);
    expectTypeOf(query).toBeCallableWith();
    expectTypeOf(query).toBeCallableWith({foo: 'bar'});
    expectTypeOf(query).toBeCallableWith(['a', 'b', 'c']);

    // should not accept non-JSON types
    // @ts-expect-error - Type 'Map<string, string>' is not assignable to type 'ReadonlyJSONValue'
    expectTypeOf(query).toBeCallableWith(new Map());
    // @ts-expect-error - Type 'Set<number>' is not assignable to type 'ReadonlyJSONValue'
    expectTypeOf(query).toBeCallableWith(new Set());
    // @ts-expect-error - Type 'Date' is not assignable to type 'ReadonlyJSONValue'
    expectTypeOf(query).toBeCallableWith(new Date());
    // @ts-expect-error - Type 'symbol' is not assignable to type 'ReadonlyJSONValue'
    expectTypeOf(query).toBeCallableWith(Symbol('test'));
  });

  test('should reject invalid validator input types', () => {
    const stringToNumberValidator = makeValidator<string, number>(data => {
      if (typeof data === 'string') {
        return parseInt(data, 10);
      }
      throw new Error('Expected string');
    });

    const validatedQuery = defineQuery(
      'validatedTest',
      {validator: stringToNumberValidator},
      ({args}) => {
        expectTypeOf(args).toEqualTypeOf<number>();
        return builder.foo;
      },
    );

    // Valid usage
    expectTypeOf(validatedQuery).toBeCallableWith('123');

    // @ts-expect-error - Type 'number' is not assignable to type 'string' (validator input)
    expectTypeOf(validatedQuery).toBeCallableWith(123);

    // @ts-expect-error - Type 'boolean' is not assignable to type 'string' (validator input)
    expectTypeOf(validatedQuery).toBeCallableWith(true);
  });

  test('should reject wrong overload usage', () => {
    // Test that 3-argument form requires options object (can't be undefined)

    // This should work (empty object is fine)
    const validThreeArg = defineQuery(
      'validThreeArg',
      {},
      ({ctx: _ctx}: {ctx: string; args: undefined}) => builder.foo,
    );

    // Note: The undefined options case is caught at the function definition level,
    // demonstrating that the type system correctly requires an options object
    // in the 3-argument form.

    void validThreeArg();
    void validThreeArg(undefined);
    // @ts-expect-error - Argument of type '32' is not assignable to parameter of type 'undefined'
    void validThreeArg(32);
  });

  test('should reject calling query with wrong argument type', () => {
    const queryWithArgs = defineQuery(
      'argsTest',
      ({ctx: _ctx, args: _args}: {ctx: string; args: string}) => builder.foo,
    );

    // @ts-expect-error - Argument of type 'number' is not assignable to parameter of type 'string'
    void queryWithArgs(123);
  });

  test('should have readonly queryName property', () => {
    const query = defineQuery(
      'readonlyNameTest',
      ({ctx: _ctx}: {ctx: string; args: undefined}) => builder.foo,
    );

    expectTypeOf(query.queryName).toEqualTypeOf<'readonlyNameTest'>();

    // @ts-expect-error - Cannot assign to 'queryName' because it is a read-only property
    query.queryName = 'different';
  });

  test('No validator and no args should allow calling with no arguments', () => {
    const query = defineQuery('noArgsNoValidator', () => builder.foo);

    // Should allow calling with no arguments
    expectTypeOf(query).toBeCallableWith();
    expectTypeOf(query).toBeCallableWith(undefined);

    query();

    // these are OK since they are ReadonlyJSONValue
    expectTypeOf(query).toBeCallableWith(123);
    expectTypeOf(query).toBeCallableWith('test');
  });
});
