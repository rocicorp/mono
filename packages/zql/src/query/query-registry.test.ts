// oxlint-disable no-explicit-any
import type {StandardSchemaV1} from '@standard-schema/spec';
import {describe, expect, expectTypeOf, test, vi} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {createBuilder} from './create-builder.ts';
import {asQueryInternals} from './query-internals.ts';
import {
  createCustomQueryBuilder,
  defineQueries,
  defineQueriesWithType,
  defineQuery,
  defineQueryWithType,
  getQuery,
  isQueryDefinition,
  isQueryRegistry,
  mustGetQuery,
  type CustomQuery,
} from './query-registry.ts';
import type {PullRow, Query} from './query.ts';

const schema = createSchema({
  tables: [
    table('foo')
      .columns({
        id: string(),
        val: number(),
      })
      .primaryKey('id'),
    table('bar')
      .columns({
        id: string(),
        val: string(),
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
  test('should work without validator parameter', () => {
    const query = defineQuery(({ctx}: {ctx: string; args: undefined}) => {
      expect(ctx).toBe('noOptionsContext');
      return builder.foo.where('id', '=', 'noOptionsId');
    });

    const result = query({args: undefined, ctx: 'noOptionsContext'});
    expect(asQueryInternals(result).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'noOptionsId'},
      },
    });
  });

  test('should work with validator that uses same type for input and output', () => {
    const query = defineQuery(
      makeValidator<number, number>(data => {
        if (typeof data === 'number') {
          return data * 2; // Transform but keep same type
        }
        throw new Error('Expected number');
      }),
      ({ctx, args}: {ctx: string; args: number}) => {
        expect(ctx).toBe('validatorContext');
        // Note: Validation happens server-side, not at query definition call time
        // So args here is the raw input, not transformed
        expect(typeof args).toBe('number');
        return builder.foo.where('val', '=', args);
      },
    );

    const result = query({args: 123, ctx: 'validatorContext'});
    expect(asQueryInternals(result).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'val'},
        op: '=',
        right: {type: 'literal', value: 123},
      },
    });
  });

  test('should work with validator that returns undefined', () => {
    const query = defineQuery(
      {
        '~standard': {
          version: 1,
          vendor: 'test',
          validate: (_data: unknown) => ({value: undefined}),
        },
      } as StandardSchemaV1<undefined, undefined>,
      ({ctx, args}: {ctx: string; args: undefined}) => {
        expect(ctx).toBe('undefinedValidatorContext');
        expect(args).toBeUndefined();
        return builder.foo.where('val', '=', 1);
      },
    );

    const result = query({args: undefined, ctx: 'undefinedValidatorContext'});
    expect(asQueryInternals(result).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'val'},
        op: '=',
        right: {type: 'literal', value: 1},
      },
    });
  });

  test('should store validator for later use', () => {
    const validator = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: (_data: unknown) => ({
          issues: [{path: ['args'], message: 'Invalid input'}],
        }),
      },
    } as StandardSchemaV1<string, string>;

    const query = defineQuery(
      validator,
      ({args}: {ctx: string; args: string}) =>
        builder.foo.where('id', '=', args),
    );

    // Validator is stored on the query definition for later use
    expect(query.validator).toBe(validator);
  });

  test('should store async validator for later use', () => {
    const validator = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: async (data: unknown) => {
          // Simulate async validation
          await new Promise(resolve => setTimeout(resolve, 1));
          return {value: `processed-${String(data)}`};
        },
      },
    } as StandardSchemaV1<string, string>;

    const query = defineQuery(
      validator,
      ({ctx, args}: {ctx: string; args: string}) => {
        expect(ctx).toBe('asyncContext');
        return builder.foo.where('id', '=', args);
      },
    );

    // Validator is stored for later use, no validation happens at call time
    expect(query.validator).toBe(validator);
    const result = query({args: 'input', ctx: 'asyncContext'});
    expect(asQueryInternals(result).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'input'},
      },
    });
  });

  test('should work with different overloads', () => {
    const query1 = defineQuery(({ctx}: {ctx: string; args: undefined}) => {
      expect(ctx).toBe('ctx1');
      return builder.foo;
    });
    const query2 = defineQuery(
      makeValidator<string, string>((x: unknown) => x as string),
      ({ctx, args}: {ctx: string; args: string}) => {
        expect(ctx).toBe('ctx2');
        expect(args).toBe('test');
        return builder.foo;
      },
    );

    // Test that all overloads actually work
    const result1 = query1({args: undefined, ctx: 'ctx1'});
    const result2 = query2({args: 'test', ctx: 'ctx2'});

    // All should return the basic table query
    [result1, result2].forEach(result => {
      expect(asQueryInternals(result).ast).toEqual({
        table: 'foo',
      });
    });
  });

  test('validator failure throws before executing query', () => {
    const validator: StandardSchemaV1<string, string> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: () => ({issues: [{message: 'bad'}]}),
      },
    };

    const run = vi.fn();

    const query = defineQuery(validator, ({args}) => {
      run(args);
      return builder.foo.where('id', '=', args);
    });

    const customQuery = createCustomQueryBuilder(
      query,
      'test',
      'boom',
      'boom',
      false,
    );

    expect(() => customQuery('boom')).toThrowErrorMatchingInlineSnapshot(
      `[Error: Validation failed for query test: bad]`,
    );
    expect(run).not.toHaveBeenCalled();
  });
});

// Type Tests
describe('defineQuery types', () => {
  test('no type annotations should treat ctx as unknown and args as ReadonlyJSONValue | undefined', () => {
    const query = defineQuery(({ctx, args}) => {
      expectTypeOf(ctx).toEqualTypeOf<unknown>();
      expectTypeOf(args).toEqualTypeOf<ReadonlyJSONValue | undefined>();
      return builder.foo.where('val', '=', 123);
    });

    // Function takes single object parameter with args and ctx
    expectTypeOf<Parameters<typeof query>>().toEqualTypeOf<
      [{args: ReadonlyJSONValue | undefined; ctx: unknown}]
    >();
  });

  test('with type annotations should respect those types', () => {
    const query = defineQuery(({ctx, args}: {ctx: string; args: number}) => {
      expectTypeOf(ctx).toEqualTypeOf<string>();
      expectTypeOf(args).toEqualTypeOf<number>();
      return builder.foo.where('val', '=', args);
    });

    // Should respect the type annotations
    expectTypeOf<Parameters<typeof query>>().toEqualTypeOf<
      [{args: number; ctx: string}]
    >();
  });

  test('validator with same input/output type', () => {
    const query = defineQuery(
      makeValidator<string, string>(data => {
        if (typeof data === 'string') {
          return data.toUpperCase();
        }
        throw new Error('Expected string');
      }),
      ({ctx: _ctx, args}: {ctx: string; args: string}) => {
        expectTypeOf(args).toEqualTypeOf<string>();
        return builder.foo.where('id', '=', args);
      },
    );

    expectTypeOf<Parameters<typeof query>>().toEqualTypeOf<
      [{args: string; ctx: string}]
    >();
  });

  test('undefined args handling', () => {
    const query = defineQuery(({args}: {args: undefined}) => {
      expectTypeOf(args).toEqualTypeOf<undefined>();
      return builder.foo;
    });

    // Should be callable with args: undefined
    expectTypeOf<Parameters<typeof query>>().toEqualTypeOf<
      [{args: undefined; ctx: unknown}]
    >();
  });

  test('should reject invalid validator input types', () => {
    // Use a validator where input is a subtype of output (satisfies TInput extends TOutput)
    const literalToStringValidator = makeValidator<'foo' | 'bar', string>(
      data => {
        if (data === 'foo' || data === 'bar') {
          return data;
        }
        throw new Error('Expected foo or bar');
      },
    );

    const validatedQuery = defineQuery(literalToStringValidator, ({args}) => {
      expectTypeOf(args).toEqualTypeOf<string>();
      return builder.foo;
    });

    // Valid usage - args is constrained to the validator's input type ('foo' | 'bar')
    expectTypeOf<Parameters<typeof validatedQuery>>().toEqualTypeOf<
      [{args: string; ctx: unknown}]
    >();

    // The following would be type errors because args must be 'foo' | 'bar'
    // but the query function receives string (the output type)
    // This demonstrates that TInput constrains what can be passed in
  });

  test('TInput and TOutput can be unrelated types', () => {
    // TInput and TOutput are independent - both just need to extend ReadonlyJSONValue | undefined
    // This allows validators to transform types arbitrarily (e.g., for Zod defaults)

    // Common case: Same types
    const query1 = defineQuery(
      makeValidator<string, string>(data => {
        if (typeof data === 'string') return data.toUpperCase();
        throw new Error('Expected string');
      }),
      ({args}) => {
        expectTypeOf(args).toEqualTypeOf<string>();
        return builder.foo.where('id', '=', args);
      },
    );
    // When called directly, pass the output type
    expectTypeOf<Parameters<typeof query1>>().toEqualTypeOf<
      [{args: string; ctx: unknown}]
    >();

    // Narrowing: literal input to wider output
    const query2 = defineQuery(
      makeValidator<'foo' | 'bar', string>(data => {
        if (data === 'foo' || data === 'bar') return data;
        throw new Error('Expected foo or bar');
      }),
      ({args}) => {
        expectTypeOf(args).toEqualTypeOf<string>();
        return builder.foo.where('id', '=', args);
      },
    );
    // When called directly, pass the output type (string, not 'foo' | 'bar')
    expectTypeOf<Parameters<typeof query2>>().toEqualTypeOf<
      [{args: string; ctx: unknown}]
    >();

    // Widening: wider input to narrower output (used for defaults)
    // Input: string | undefined, Output: string
    const defaultValidator = makeValidator<string | undefined, string>(data =>
      data === undefined ? 'default' : (data as string),
    );
    const query3 = defineQuery(defaultValidator, ({args}) => {
      expectTypeOf(args).toEqualTypeOf<string>();
      return builder.foo.where('id', '=', args);
    });
    // When called directly, pass the output type (string, not string | undefined)
    expectTypeOf<Parameters<typeof query3>>().toEqualTypeOf<
      [{args: string; ctx: unknown}]
    >();
    // Verify the validator's input type accepts undefined
    expectTypeOf<
      Parameters<(typeof defaultValidator)['~standard']['validate']>
    >().toEqualTypeOf<[unknown]>();

    // Transform: completely different types
    const stringToNumberValidator = makeValidator<string, number>(data => {
      if (typeof data === 'string') {
        const num = parseInt(data, 10);
        if (!isNaN(num)) return num;
      }
      throw new Error('Expected numeric string');
    });
    const query4 = defineQuery(stringToNumberValidator, ({args}) => {
      expectTypeOf(args).toEqualTypeOf<number>();
      return builder.foo.where('val', '=', args);
    });
    // When called directly, pass the output type (number, not string)
    expectTypeOf<Parameters<typeof query4>>().toEqualTypeOf<
      [{args: number; ctx: unknown}]
    >();
    // Verify the validator's input type accepts string
    expectTypeOf<
      Parameters<(typeof stringToNumberValidator)['~standard']['validate']>
    >().toEqualTypeOf<[unknown]>();
  });

  test('defineQuery with with explicit types', () => {
    type Context = {userId: string};

    const query = defineQuery<
      {id: string},
      Context,
      typeof schema,
      'foo',
      PullRow<'foo', typeof schema>
    >(({args}) => builder.foo.where('id', '=', args.id));

    expectTypeOf<typeof query>().returns.toEqualTypeOf<
      Query<'foo', Schema, PullRow<'foo', typeof schema>>
    >();
    expectTypeOf<typeof query>()
      .parameter(0)
      .toEqualTypeOf<{args: {id: string}; ctx: Context}>();
  });

  test('defineQuery infers args and context types', () => {
    const query = defineQuery(({args, ctx}) => {
      expectTypeOf(args).toEqualTypeOf<ReadonlyJSONValue | undefined>();
      expectTypeOf(ctx).toEqualTypeOf<unknown>();
      return builder.foo.where('id', '=', 'test');
    });

    expectTypeOf<typeof query>().returns.toEqualTypeOf<
      Query<'foo', Schema, PullRow<'foo', typeof schema>>
    >();
    expectTypeOf<typeof query>()
      .parameter(0)
      .toEqualTypeOf<{args: ReadonlyJSONValue | undefined; ctx: unknown}>();
  });
});

describe('defineQueries', () => {
  test('should support (args).toQuery(ctx)', () => {
    const queries = defineQueries({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    const result = queries.getUser('test-id').toQuery({userId: 'ctx-user'});
    expect(asQueryInternals(result).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'test-id'},
      },
    });
  });

  test('should support nested query definitions', () => {
    const queries = defineQueries({
      users: {
        getById: defineQuery(
          ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
            builder.foo.where('id', '=', args),
        ),
      },
    });

    const result = queries.users
      .getById('nested-id')
      .toQuery({userId: 'ctx-user'});
    expect(asQueryInternals(result).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'nested-id'},
      },
    });
  });

  test('should validate args when calling with args', () => {
    const queries = defineQueries({
      getUser: defineQuery(
        makeValidator<string, string>(data => {
          if (typeof data !== 'string') {
            throw new Error('Expected string');
          }
          return data.toUpperCase();
        }),
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    const result = queries.getUser('test-id').toQuery({userId: 'ctx-user'});
    expect(asQueryInternals(result).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'TEST-ID'},
      },
    });
  });

  test('should throw error when calling args twice', () => {
    const queries = defineQueries({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    // oxlint-disable-next-line no-explicit-any -- testing runtime error
    const q = queries.getUser('first') as any;
    expect(() => q('second')).toThrow('args already set');
  });

  test('should throw error when calling toQuery without args', () => {
    const queries = defineQueries({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    // oxlint-disable-next-line no-explicit-any -- testing runtime error
    const q = queries.getUser as any;
    expect(() => q.toQuery({userId: 'user'})).toThrow('args not set');
  });

  test('should allow optional args when Args type is undefined', () => {
    const queries = defineQueries({
      getAllUsers: defineQuery(
        ({ctx: _ctx}: {args: undefined; ctx: {userId: string}}) => builder.foo,
      ),
    });

    // Should work without calling with args
    const result1 = queries.getAllUsers().toQuery({userId: 'ctx-user'});
    expect(asQueryInternals(result1).ast).toEqual({
      table: 'foo',
    });

    // Should also work with explicit undefined
    const result2 = queries
      .getAllUsers(undefined)
      .toQuery({userId: 'ctx-user'});
    expect(asQueryInternals(result2).ast).toEqual({
      table: 'foo',
    });
  });

  test('should accept input type and send input args to server when input !== output', () => {
    // Validator transforms string to number
    const stringToNumberValidator = makeValidator<string, number>(data => {
      const num = parseInt(data as string, 10);
      if (isNaN(num)) throw new Error('Expected numeric string');
      return num;
    });

    const queries = defineQueries({
      getByVal: defineQuery(
        stringToNumberValidator,
        ({args}: {args: number; ctx: unknown}) =>
          builder.foo.where('val', '=', args),
      ),
    });

    // Type test: callable should accept string (input type), not number (output type)
    expectTypeOf(queries.getByVal).parameter(0).toEqualTypeOf<string>();

    // Call with string input
    const result = queries.getByVal('42').toQuery({});

    // The query AST should use the transformed value (number) for the where clause
    expect(asQueryInternals(result).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'val'},
        op: '=',
        right: {type: 'literal', value: 42}, // transformed to number
      },
    });

    // But the args sent to server (in customQueryID) should be the original input (string)
    expect(asQueryInternals(result).customQueryID).toEqual({
      name: 'getByVal',
      args: ['42'], // original string, not transformed number
    });
  });

  test('should handle validator with default value transform', () => {
    // Validator provides default when input is undefined
    const withDefaultValidator = makeValidator<string | undefined, string>(
      data => (data === undefined ? 'default-value' : (data as string)),
    );

    const queries = defineQueries({
      getValue: defineQuery(
        withDefaultValidator,
        ({args}: {args: string; ctx: unknown}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    // Type test: callable should accept string | undefined (input type)
    expectTypeOf(queries.getValue)
      .parameter(0)
      .toEqualTypeOf<string | undefined>();

    // Call with undefined - query function gets transformed value
    const result = queries.getValue(undefined).toQuery({});

    // The query AST should use the transformed value
    expect(asQueryInternals(result).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'default-value'}, // transformed
      },
    });

    // Args sent to server: undefined args produce empty array (existing behavior)
    expect(asQueryInternals(result).customQueryID).toEqual({
      name: 'getValue',
      args: [], // undefined becomes empty array
    });

    // Call with actual string value
    const result2 = queries.getValue('explicit').toQuery({});

    // The query AST should use the input value (no transform needed)
    expect(asQueryInternals(result2).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'explicit'},
      },
    });

    // Args sent to server should be the original input string
    expect(asQueryInternals(result2).customQueryID).toEqual({
      name: 'getValue',
      args: ['explicit'],
    });
  });

  test('defineQueries infers types from defineQuery and provides Schema type', () => {
    type Context1 = {userId: string};
    type Context2 = {some: 'baz'};

    const define = defineQueriesWithType<typeof schema>();

    const getFoo = defineQuery<
      {id: string},
      Context1,
      typeof schema,
      'foo',
      PullRow<'foo', typeof schema>
    >(({ctx, args}) => {
      expectTypeOf(ctx).toEqualTypeOf<Context1>();
      expectTypeOf(args).toEqualTypeOf<{id: string}>();

      return builder.foo;
    });

    const getBar = defineQuery<
      {some: 'baz'},
      Context2,
      typeof schema,
      'bar',
      PullRow<'bar', typeof schema>
    >(({ctx, args}) => {
      expectTypeOf(ctx).toEqualTypeOf<Context2>();
      expectTypeOf(args).toEqualTypeOf<{some: 'baz'}>();

      return builder.bar;
    });

    const queries = define({
      getFoo,
      getBar,
    });

    expectTypeOf<typeof queries.getFoo>().returns.toExtend<
      CustomQuery<
        'foo',
        {id: string},
        typeof schema,
        PullRow<'foo', typeof schema>,
        Context1,
        true
      >
    >();
    expectTypeOf<typeof queries.getFoo>()
      .parameter(0)
      .toEqualTypeOf<{id: string}>();
    expectTypeOf<ReturnType<typeof queries.getFoo>['toQuery']>()
      .parameter(0)
      .toEqualTypeOf<Context1>();

    expectTypeOf<typeof queries.getBar>().returns.toExtend<
      CustomQuery<
        'bar',
        {some: 'baz'},
        typeof schema,
        PullRow<'bar', typeof schema>,
        Context2,
        true
      >
    >();
    expectTypeOf<typeof queries.getBar>()
      .parameter(0)
      .toEqualTypeOf<{some: 'baz'}>();
    expectTypeOf<ReturnType<typeof queries.getBar>['toQuery']>()
      .parameter(0)
      .toEqualTypeOf<Context2>();
  });
});

describe('isQueryRegistry', () => {
  test('returns true for query registry created with defineQueries', () => {
    const queries = defineQueries({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    expect(isQueryRegistry(queries)).toBe(true);
  });

  test('returns true for nested query registry', () => {
    const queries = defineQueries({
      users: {
        getById: defineQuery(
          ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
            builder.foo.where('id', '=', args),
        ),
      },
    });

    expect(isQueryRegistry(queries)).toBe(true);
  });

  test.for([
    ['null', null],
    ['undefined', undefined],
    ['empty object', {}],
    ['plain object', {foo: 'bar'}],
    ['string', 'string'],
    ['number', 123],
    ['boolean', true],
    ['empty array', []],
    ['array', [1, 2, 3]],
    ['function', () => {}],
  ] as const)('returns false for %s', ([_name, value]) => {
    expect(isQueryRegistry(value)).toBe(false);
  });
});

describe('defineQueries types', () => {
  test('defineQueries keeps validator input for callable and context for toQuery', () => {
    type Context = {userId: string};
    const stringToNumberValidator = makeValidator<
      {raw: string},
      {parsed: number}
    >(data => {
      const value = Number((data as {raw: string}).raw);
      if (Number.isNaN(value)) {
        throw new Error('invalid');
      }
      return {parsed: value};
    });

    const queries = defineQueries({
      byCount: defineQuery(
        stringToNumberValidator,
        ({args}: {ctx: Context; args: {parsed: number}}) =>
          builder.foo.where('val', '=', args.parsed),
      ),
    });

    expectTypeOf(queries.byCount).parameter(0).toEqualTypeOf<{raw: string}>();

    const withArgs = queries.byCount({raw: '1'});
    expectTypeOf<Parameters<typeof withArgs.toQuery>>().toEqualTypeOf<
      [Context]
    >();
  });

  test('initial CustomQuery should have args callable but no toQuery', () => {
    const queries = defineQueries({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    // Should be callable (to set args)
    expectTypeOf<Parameters<typeof queries.getUser>>().toEqualTypeOf<
      [args: string]
    >();

    // Should NOT have context method
    expectTypeOf(queries.getUser).not.toHaveProperty('context');

    // Should NOT have toQuery method (args not set)
    expectTypeOf(queries.getUser).not.toHaveProperty('toQuery');
  });

  test('after setting args, should have toQuery but not be callable again', () => {
    const queries = defineQueries({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    const withArgs = queries.getUser('test-id');

    // Should have toQuery method that takes context
    expectTypeOf<Parameters<typeof withArgs.toQuery>>().toEqualTypeOf<
      [{userId: string}]
    >();

    // Should NOT be callable - verify it's not a function type
    expectTypeOf<typeof withArgs>().not.toMatchTypeOf<
      (args: string) => unknown
    >();
  });

  test('args type should be enforced', () => {
    const queries = defineQueries({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    // Correct args type
    expectTypeOf<Parameters<typeof queries.getUser>>().toEqualTypeOf<
      [args: string]
    >();
  });

  test('context type should be enforced on toQuery', () => {
    const queries = defineQueries({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    const withArgs = queries.getUser('test-id');

    // Correct context type
    expectTypeOf<Parameters<typeof withArgs.toQuery>>().toEqualTypeOf<
      [{userId: string}]
    >();
  });

  test('Context inferred as unknown when not annotated', () => {
    const define = defineQueriesWithType<typeof schema>();

    const queries = define({
      getFoo: defineQuery(
        makeValidator<string, string>(() => ''),
        ({args}) => builder.foo.where('id', '=', args),
      ),
      getBar: defineQuery(
        makeValidator<undefined, undefined>(() => undefined),
        () => builder.bar,
      ),
    });
    const withArgs = queries.getFoo('test-id');

    // Context is unknown, so any value is accepted
    expectTypeOf(withArgs.toQuery).toEqualTypeOf<
      (ctx: unknown) => Query<'foo', typeof schema>
    >();
  });
});

describe('context type mismatch detection', () => {
  test('each query keeps its own context type', () => {
    // Two queries with DIFFERENT explicit context types
    const queries = defineQueries({
      query1: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
      query2: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {role: 'admin' | 'user'}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    expectTypeOf<(typeof queries)['query1']['~']['$context']>().toEqualTypeOf<{
      userId: string;
    }>();
    expectTypeOf<(typeof queries)['query2']['~']['$context']>().toEqualTypeOf<{
      role: 'admin' | 'user';
    }>();

    const q1 = queries.query1('test');
    const q2 = queries.query2('test');

    // Each query uses its own specific context type
    // q1 only needs userId
    expectTypeOf<Parameters<typeof q1.toQuery>>().toEqualTypeOf<
      [{userId: string}]
    >();
    // q2 only needs role
    expectTypeOf<Parameters<typeof q2.toQuery>>().toEqualTypeOf<
      [{role: 'admin' | 'user'}]
    >();
  });
});

describe('defineQueries merging', () => {
  test('should merge base and overrides at runtime', () => {
    const base = defineQueries({
      queryA: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
      queryB: defineQuery(
        ({args, ctx: _ctx}: {args: number; ctx: {userId: string}}) =>
          builder.foo.where('val', '=', args),
      ),
    });

    const extended = defineQueries(base, {
      queryC: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.bar.where('id', '=', args),
      ),
    });

    // All queries should be present
    expect('queryA' in extended).toBe(true);
    expect('queryB' in extended).toBe(true);
    expect('queryC' in extended).toBe(true);

    // Queries should work correctly
    const resultA = extended.queryA('test-a').toQuery({userId: 'user1'});
    expect(asQueryInternals(resultA).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'test-a'},
      },
    });

    const resultC = extended.queryC('test-c').toQuery({userId: 'user1'});
    expect(asQueryInternals(resultC).ast).toEqual({
      table: 'bar',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'test-c'},
      },
    });
  });

  test('should override existing queries', () => {
    const base = defineQueries({
      queryA: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
      queryB: defineQuery(
        ({args, ctx: _ctx}: {args: number; ctx: {userId: string}}) =>
          builder.foo.where('val', '=', args),
      ),
    });

    const extended = defineQueries(base, {
      // Override queryB with a different implementation
      queryB: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.bar.where('val', '=', args),
      ),
    });

    // queryA should still work as before
    const resultA = extended.queryA('test-a').toQuery({userId: 'user1'});
    expect(asQueryInternals(resultA).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'test-a'},
      },
    });

    // queryB should use the new implementation (bar table, string args)
    const resultB = extended.queryB('overridden').toQuery({userId: 'user1'});
    expect(asQueryInternals(resultB).ast).toEqual({
      table: 'bar',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'val'},
        op: '=',
        right: {type: 'literal', value: 'overridden'},
      },
    });
  });

  test('merged registry should be a valid query registry', () => {
    const base = defineQueries({
      queryA: defineQuery(({args: _args}: {args: undefined}) => builder.foo),
    });

    const extended = defineQueries(base, {
      queryB: defineQuery(({args: _args}: {args: undefined}) => builder.bar),
    });

    expect(isQueryRegistry(extended)).toBe(true);
  });

  test('should merge two plain query definitions at runtime', () => {
    // This tests the overload: defineQueries(defs1, defs2)
    // where both are plain QueryDefinitions, not a QueryRegistry
    const defs1 = {
      queryA: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
      queryB: defineQuery(
        ({args, ctx: _ctx}: {args: number; ctx: {userId: string}}) =>
          builder.foo.where('val', '=', args),
      ),
    };

    const defs2 = {
      queryC: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.bar.where('id', '=', args),
      ),
    };

    const merged = defineQueries(defs1, defs2);

    // All queries should be present
    expect('queryA' in merged).toBe(true);
    expect('queryB' in merged).toBe(true);
    expect('queryC' in merged).toBe(true);

    // Queries should work correctly
    const resultA = merged.queryA('test-a').toQuery({userId: 'user1'});
    expect(asQueryInternals(resultA).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'test-a'},
      },
    });

    const resultC = merged.queryC('test-c').toQuery({userId: 'user1'});
    expect(asQueryInternals(resultC).ast).toEqual({
      table: 'bar',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'test-c'},
      },
    });

    expect(isQueryRegistry(merged)).toBe(true);
  });

  test('should merge two plain query definitions with overrides', () => {
    const defs1 = {
      queryA: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
      queryB: defineQuery(
        ({args, ctx: _ctx}: {args: number; ctx: {userId: string}}) =>
          builder.foo.where('val', '=', args),
      ),
    };

    const defs2 = {
      // Override queryB with a different implementation
      queryB: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.bar.where('val', '=', args),
      ),
    };

    const merged = defineQueries(defs1, defs2);

    // queryA should still work as before
    const resultA = merged.queryA('test-a').toQuery({userId: 'user1'});
    expect(asQueryInternals(resultA).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'test-a'},
      },
    });

    // queryB should use the new implementation (bar table, string args)
    const resultB = merged.queryB('overridden').toQuery({userId: 'user1'});
    expect(asQueryInternals(resultB).ast).toEqual({
      table: 'bar',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'val'},
        op: '=',
        right: {type: 'literal', value: 'overridden'},
      },
    });
  });
});

describe('defineQueries merging types', () => {
  test('merged type should include all queries from base and overrides', () => {
    const base = defineQueries({
      queryA: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
      queryB: defineQuery(
        ({args, ctx: _ctx}: {args: number; ctx: {userId: string}}) =>
          builder.foo.where('val', '=', args),
      ),
    });

    const extended = defineQueries(base, {
      queryC: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.bar.where('id', '=', args),
      ),
    });

    // All queries should be accessible with correct types
    expectTypeOf<Parameters<typeof extended.queryA>>().toEqualTypeOf<
      [args: string]
    >();
    expectTypeOf<Parameters<typeof extended.queryB>>().toEqualTypeOf<
      [args: number]
    >();
    expectTypeOf<Parameters<typeof extended.queryC>>().toEqualTypeOf<
      [args: string]
    >();
  });

  test('override should change the type of the query', () => {
    const base = defineQueries({
      queryA: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
      queryB: defineQuery(
        ({args, ctx: _ctx}: {args: number; ctx: {userId: string}}) =>
          builder.foo.where('val', '=', args),
      ),
    });

    const extended = defineQueries(base, {
      // Override queryB with different args type
      queryB: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.bar.where('val', '=', args),
      ),
    });

    // queryA keeps its original type
    expectTypeOf<Parameters<typeof extended.queryA>>().toEqualTypeOf<
      [args: string]
    >();

    // queryB now has string args instead of number
    expectTypeOf<Parameters<typeof extended.queryB>>().toEqualTypeOf<
      [args: string]
    >();
  });

  test('toQuery context types should be preserved', () => {
    const base = defineQueries({
      queryA: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    const extended = defineQueries(base, {
      queryB: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {role: string}}) =>
          builder.bar.where('id', '=', args),
      ),
    });

    const qA = extended.queryA('test');
    const qB = extended.queryB('test');

    expectTypeOf<Parameters<typeof qA.toQuery>>().toEqualTypeOf<
      [{userId: string}]
    >();
    expectTypeOf<Parameters<typeof qB.toQuery>>().toEqualTypeOf<
      [{role: string}]
    >();
  });

  test('return type should be Query with correct table', () => {
    const b = {
      getFoo: defineQuery(({args: _args}: {args: undefined}) => builder.foo),
    };
    const base = defineQueries<typeof b, typeof schema>(b);

    const e = {
      getBar: defineQuery(({args: _args}: {args: undefined}) => builder.bar),
    };
    const extended = defineQueries<typeof b, typeof e, typeof schema>(base, e);

    const fooQuery = extended.getFoo().toQuery({});
    const barQuery = extended.getBar().toQuery({});

    expectTypeOf(fooQuery).toEqualTypeOf<Query<'foo', typeof schema>>();
    expectTypeOf(barQuery).toEqualTypeOf<Query<'bar', typeof schema>>();
  });

  test('merging two plain query definitions should have correct types', () => {
    // This tests the overload: defineQueries(defs1, defs2)
    // where both are plain QueryDefinitions, not a QueryRegistry
    const defs1 = {
      queryA: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
      queryB: defineQuery(
        ({args, ctx: _ctx}: {args: number; ctx: {userId: string}}) =>
          builder.foo.where('val', '=', args),
      ),
    };

    const defs2 = {
      queryC: defineQuery(
        ({args, ctx: _ctx}: {args: boolean; ctx: {userId: string}}) =>
          builder.bar.where('id', '=', args ? 'yes' : 'no'),
      ),
    };

    const merged = defineQueries(defs1, defs2);

    // All queries should be accessible with correct types
    expectTypeOf<Parameters<typeof merged.queryA>>().toEqualTypeOf<
      [args: string]
    >();
    expectTypeOf<Parameters<typeof merged.queryB>>().toEqualTypeOf<
      [args: number]
    >();
    expectTypeOf<Parameters<typeof merged.queryC>>().toEqualTypeOf<
      [args: boolean]
    >();
  });

  test('merging two plain query definitions with overrides should have correct types', () => {
    const defs1 = {
      queryA: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
      queryB: defineQuery(
        ({args, ctx: _ctx}: {args: number; ctx: {userId: string}}) =>
          builder.foo.where('val', '=', args),
      ),
    };

    const defs2 = {
      // Override queryB with a different type
      queryB: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.bar.where('val', '=', args),
      ),
    };

    const merged = defineQueries(defs1, defs2);

    // queryA keeps its original type
    expectTypeOf<Parameters<typeof merged.queryA>>().toEqualTypeOf<
      [args: string]
    >();

    // queryB now has string args instead of number
    expectTypeOf<Parameters<typeof merged.queryB>>().toEqualTypeOf<
      [args: string]
    >();
  });

  test('deep merge preserves nested query types', () => {
    const base = defineQueries({
      users: {
        byId: defineQuery(({args}: {args: {id: string}}) =>
          builder.foo.where('id', '=', args.id),
        ),
        byVal: defineQuery(({args}: {args: {val: number}}) =>
          builder.foo.where('val', '=', args.val),
        ),
      },
      posts: {
        all: defineQuery(() => builder.bar),
      },
    });

    const extended = defineQueries(base, {
      users: {
        byEmail: defineQuery(({args}: {args: {email: string}}) =>
          builder.foo.where('id', '=', args.email),
        ),
      },
    });

    // Original nested queries should be preserved with correct types
    expectTypeOf<Parameters<typeof extended.users.byId>>().toEqualTypeOf<
      [args: {id: string}]
    >();
    expectTypeOf<Parameters<typeof extended.users.byVal>>().toEqualTypeOf<
      [args: {val: number}]
    >();

    // New nested query should be available
    expectTypeOf<Parameters<typeof extended.users.byEmail>>().toEqualTypeOf<
      [args: {email: string}]
    >();

    // Other namespaces preserved - query with no args is callable without arguments
    expectTypeOf(extended.posts.all).toBeCallableWith();
  });

  test('deep merge overrides nested queries correctly', () => {
    const base = defineQueries({
      users: {
        byId: defineQuery(({args}: {args: {id: string}}) =>
          builder.foo.where('id', '=', args.id),
        ),
      },
    });

    const extended = defineQueries(base, {
      users: {
        // Override byId with different args type
        byId: defineQuery(({args}: {args: {id: string; extra: boolean}}) =>
          builder.bar.where('id', '=', args.id),
        ),
      },
    });

    // Overridden query has new type
    expectTypeOf<Parameters<typeof extended.users.byId>>().toEqualTypeOf<
      [args: {id: string; extra: boolean}]
    >();
  });
});

describe('getQuery', () => {
  test('returns query by name', () => {
    const queries = defineQueries({
      getUser: defineQuery(({args}: {args: string}) =>
        builder.foo.where('id', '=', args),
      ),
      nested: {
        getBar: defineQuery(() => builder.bar),
      },
    });

    const query1 = getQuery(queries, 'getUser');
    const query2 = getQuery(queries, 'nested.getBar');

    expect(query1).toBe(queries.getUser);
    expect(query2).toBe(queries.nested.getBar);

    assert(query1);
    assert(query2);

    // The runtime of this property is not the same as the type.
    expect(query1['~']).toEqual('CustomQuery');
    expectTypeOf(query1['~']['$tableName']).toEqualTypeOf<'foo' | 'bar'>();
    expectTypeOf(query1['~']['$schema']).toEqualTypeOf<Schema>();
    expectTypeOf(query1['~']['$input']).toEqualTypeOf<
      ReadonlyJSONValue | undefined
    >();
    expectTypeOf(query1['~']['$context']).toEqualTypeOf<unknown>();
    expectTypeOf(query1['~']['$return']).toEqualTypeOf<
      | {
          readonly id: string;
          readonly val: number;
        }
      | {
          readonly id: string;
          readonly val: string;
        }
    >();
    expectTypeOf(query1['~']['$hasArgs']).toEqualTypeOf<false>();

    expectTypeOf(query2['~']['$tableName']).toEqualTypeOf<'foo' | 'bar'>();
    expectTypeOf(query2['~']['$schema']).toEqualTypeOf<Schema>();
    expectTypeOf(query2['~']['$input']).toEqualTypeOf<
      ReadonlyJSONValue | undefined
    >();
    expectTypeOf(query2['~']['$context']).toEqualTypeOf<unknown>();
    expectTypeOf(query2['~']['$hasArgs']).toEqualTypeOf<false>();
  });

  test('returns undefined for non-existent query', () => {
    const queries = defineQueries({
      getUser: defineQuery(() => builder.foo),
    });

    expect(getQuery(queries, 'nonExistent')).toBeUndefined();
    expect(getQuery(queries, 'nested.getBar')).toBeUndefined();
  });
});

describe('mustGetQuery', () => {
  test('returns query by name', () => {
    type Context1 = {userId: string};
    const defineQueriesTyped = defineQueriesWithType<typeof schema>();

    const queries = defineQueriesTyped({
      getUser: defineQuery(({args}: {args: string; ctx: Context1}) =>
        builder.foo.where('id', '=', args),
      ),
      nested: {
        getBar: defineQuery(() => builder.bar),
      },
    });

    const query1 = mustGetQuery(queries, 'getUser');
    const query2 = mustGetQuery(queries, 'nested.getBar');

    expect(query1).toBe(queries.getUser);
    expect(query2).toBe(queries.nested.getBar);
    expectTypeOf(query1['~']['$tableName']).toEqualTypeOf<'foo' | 'bar'>();
    expectTypeOf(query2['~']['$tableName']).toEqualTypeOf<'foo' | 'bar'>();
    expectTypeOf(query1['~']['$input']).toEqualTypeOf<
      ReadonlyJSONValue | undefined
    >();
    expectTypeOf(query1['~']['$context']).toEqualTypeOf<unknown>();

    expectTypeOf(query2['~']['$schema']).toEqualTypeOf<typeof schema>();
  });

  test('returns query by name with schema and context', () => {
    type Context = {userId: string};
    const defineQueriesTyped = defineQueriesWithType<typeof schema>();

    const queries = defineQueriesTyped({
      getUser: defineQueryWithType<typeof schema, Context>()(
        ({args}: {args: string}) => builder.foo.where('id', '=', args),
      ),
      nested: {
        getBar: defineQueryWithType<typeof schema, Context>()(
          () => builder.bar,
        ),
      },
    });

    const query1 = mustGetQuery(queries, 'getUser');
    const query2 = mustGetQuery(queries, 'nested.getBar');

    expectTypeOf(query1['~']['$context']).toEqualTypeOf<Context>();

    expectTypeOf(query2['~']['$schema']).toEqualTypeOf<typeof schema>();
  });

  test('throws for non-existent query', () => {
    const queries = defineQueries({
      getUser: defineQuery(() => builder.foo),
    });

    expect(() => mustGetQuery(queries, 'nonExistent')).toThrow(
      'Query not found: nonExistent',
    );
  });
});

describe('defineQueryWithType', () => {
  test('binds schema and context types correctly', () => {
    type Context = {userId: string; role: 'admin' | 'user'};

    const defineQueryTyped = defineQueryWithType<typeof schema, Context>();

    const query = defineQueryTyped<
      {id: string},
      PullRow<'foo', typeof schema>,
      'foo'
    >(({args, ctx}) => {
      expectTypeOf(args).toEqualTypeOf<{id: string}>();
      expectTypeOf(ctx).toEqualTypeOf<Context>();
      return builder.foo.where('id', '=', args.id);
    });

    const defineQueriesTyped = defineQueriesWithType<typeof schema>();

    const queries = defineQueriesTyped({test: query});

    const q = queries.test({id: 'test'});

    expectTypeOf<(typeof q)['~']['$schema']>().toEqualTypeOf<typeof schema>();
    expectTypeOf<(typeof q)['~']['$context']>().toEqualTypeOf<Context>();
    expectTypeOf<(typeof q)['~']['$return']>().toEqualTypeOf<
      PullRow<'foo', typeof schema>
    >();
  });

  test('works with validator', () => {
    type Context = {userId: string};
    const define = defineQueryWithType<typeof schema, Context>();

    const query = define(
      ((v: unknown) => v) as unknown as StandardSchemaV1<
        {raw: number},
        {validated: string}
      >,
      ({args, ctx}) => {
        expectTypeOf(args).toEqualTypeOf<{validated: string}>();
        expectTypeOf(ctx).toEqualTypeOf<Context>();
        return builder.foo.where('id', '=', args.validated);
      },
    );

    const defineQueriesTyped = defineQueriesWithType<typeof schema>();
    const queries = defineQueriesTyped({test: query});

    const q = queries.test({raw: 1});

    expectTypeOf<(typeof q)['~']['$schema']>().toEqualTypeOf<typeof schema>();
    expectTypeOf<(typeof q)['~']['$context']>().toEqualTypeOf<Context>();
    expectTypeOf<(typeof q)['~']['$input']>().toEqualTypeOf<{
      raw: number;
    }>();
    expectTypeOf<(typeof q)['~']['$return']>().toEqualTypeOf<{
      readonly id: string;
      readonly val: number;
    }>();
  });

  test('context-only overload still produces a working query', () => {
    type Ctx = {tenant: string};
    const defineQueryTyped = defineQueryWithType<Ctx>();

    const query = defineQueryTyped(({ctx}) => {
      expect(ctx.tenant).toBe('t1');
      return builder.foo;
    });

    const result = query({args: undefined, ctx: {tenant: 't1'}});
    expect(asQueryInternals(result).ast).toEqual({table: 'foo'});
  });
});

describe('defineQueriesWithType', () => {
  test('binds schema type correctly', () => {
    const define = defineQueriesWithType<typeof schema>();

    const queries = define({
      getFoo: defineQuery(({args}: {args: string}) =>
        builder.foo.where('id', '=', args),
      ),
      getBar: defineQuery(() => builder.bar),
    });

    const q = queries.getFoo('test');

    expectTypeOf<(typeof q)['~']['$schema']>().toEqualTypeOf<typeof schema>();
  });

  test('preserves different context types per query', () => {
    type Context1 = {userId: string};
    type Context2 = {role: 'admin'};

    const define = defineQueriesWithType<typeof schema>();

    const queries = define({
      query1: defineQuery(
        ({ctx: _ctx}: {ctx: Context1; args: undefined}) => builder.foo,
      ),
      query2: defineQuery(
        ({ctx: _ctx}: {ctx: Context2; args: undefined}) => builder.bar,
      ),
    });

    const q1 = queries.query1();
    const q2 = queries.query2();

    expectTypeOf<(typeof q1)['~']['$context']>().toEqualTypeOf<Context1>();
    expectTypeOf<(typeof q2)['~']['$context']>().toEqualTypeOf<Context2>();
  });
});

describe('isQueryDefinition', () => {
  test('detects query definitions and rejects other values', () => {
    const def = defineQuery(() => builder.foo);

    expect(isQueryDefinition(def)).toBe(true);
    expect(isQueryDefinition(() => builder.foo)).toBe(false);
    expect(isQueryDefinition({validator: undefined})).toBe(false);
    expect(isQueryDefinition(null)).toBe(false);
  });
});

test('defineQueries preserves types from defineQuery', () => {
  type Context1 = {userId: string};
  type Context2 = {some: 'baz'};
  type Context3 = {bool: true};

  const queries = defineQueries({
    // Explicit type params
    def1: defineQuery<
      {id: string},
      Context1,
      typeof schema,
      'foo',
      PullRow<'foo', typeof schema>
    >(({args, ctx}) => {
      expectTypeOf(args).toEqualTypeOf<{id: string}>();
      expectTypeOf(ctx).toEqualTypeOf<Context1>();
      return builder.foo.where('id', '=', args.id);
    }),
    // Different table, different context
    def2: defineQuery<
      {id: string},
      Context2,
      typeof schema,
      'bar',
      PullRow<'bar', typeof schema>
    >(({args, ctx}) => {
      expectTypeOf(args).toEqualTypeOf<{id: string}>();
      expectTypeOf(ctx).toEqualTypeOf<Context2>();
      return builder.bar.where('id', '=', args.id);
    }),
    // No explicit type params - should infer defaults
    def3: defineQuery(({args, ctx}) => {
      expectTypeOf(args).toEqualTypeOf<ReadonlyJSONValue | undefined>();
      expectTypeOf(ctx).toEqualTypeOf<unknown>();
      return builder.foo;
    }),
    // Inline type annotation instead of type params
    def4: defineQuery(({args, ctx}: {args: string; ctx: Context3}) => {
      expectTypeOf(args).toEqualTypeOf<string>();
      expectTypeOf(ctx).toEqualTypeOf<Context3>();
      return builder.bar.where('id', '=', args);
    }),
  });

  expectTypeOf(queries.def1).parameter(0).toEqualTypeOf<{id: string}>();

  const query1 = queries.def1({id: '123'});
  expectTypeOf(query1['~']['$input']).toEqualTypeOf<{id: string}>();
  expectTypeOf(query1['~']['$context']).toEqualTypeOf<Context1>();
  expectTypeOf(query1['~']['$return']).toEqualTypeOf<
    PullRow<'foo', typeof schema>
  >();

  const query2 = queries.def2({id: '123'});
  expectTypeOf(query2['~']['$input']).toEqualTypeOf<{id: string}>();
  expectTypeOf(query2['~']['$context']).toEqualTypeOf<Context2>();
  expectTypeOf(query2['~']['$return']).toEqualTypeOf<
    PullRow<'bar', typeof schema>
  >();

  expectTypeOf(queries.def3)
    .parameter(0)
    .toEqualTypeOf<ReadonlyJSONValue | undefined>();
  const query3 = queries.def3('whatever');
  expectTypeOf(query3['~']['$input']).toEqualTypeOf<
    ReadonlyJSONValue | undefined
  >();
  expectTypeOf(query3['~']['$context']).toEqualTypeOf<unknown>();

  const query4 = queries.def4('test-string');
  expectTypeOf(query4['~']['$input']).toEqualTypeOf<string>();
  expectTypeOf(query4['~']['$context']).toEqualTypeOf<Context3>();
});
