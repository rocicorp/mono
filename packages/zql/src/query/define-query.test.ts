// oxlint-disable no-explicit-any
import type {StandardSchemaV1} from '@standard-schema/spec';
import {describe, expect, expectTypeOf, test} from 'vitest';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from './create-builder.ts';
import {
  defineQueries,
  defineQueriesWithType,
  defineQuery,
  type ContextTypeOfCustomQueries,
} from './define-query.ts';
import {asQueryInternals} from './query-internals.ts';
import type {Query} from './query.ts';

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
});

describe('defineQueries types', () => {
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
    const queries = defineQueries({
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
      (ctx: any) => Query<typeof schema, 'foo'>
    >();
  });
});

describe('defineQueriesWithType', () => {
  test('allows explicit Context type with factory pattern', () => {
    type MyContext = {userId: string; role: 'admin' | 'user'};

    const queries = defineQueriesWithType<MyContext>()({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: MyContext}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    const withArgs = queries.getUser('test-id');

    // Context type is enforced
    expectTypeOf<Parameters<typeof withArgs.toQuery>>().toEqualTypeOf<
      [{userId: string; role: 'admin' | 'user'}]
    >();
  });

  test('allows explicit Schema and Context types', () => {
    type MyContext = 'my-context';

    const queries = defineQueriesWithType<typeof schema, MyContext>()({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: MyContext}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    const withArgs = queries.getUser('test-id');

    expectTypeOf<Parameters<typeof withArgs.toQuery>>().toEqualTypeOf<
      ['my-context']
    >();
  });

  test('runtime behavior matches defineQueries', () => {
    type MyContext = {userId: string};

    const queries = defineQueriesWithType<MyContext>()({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: MyContext}) =>
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

  test('works without type annotations on defineQuery', () => {
    const queries = defineQueriesWithType<{userId: string}>()({
      getUser: defineQuery(
        makeValidator(v => v as string | undefined),
        ({args, ctx}) => builder.foo.where('id', '=', args ?? ctx.userId),
      ),
    });

    expectTypeOf<Parameters<typeof queries.getUser>>().toEqualTypeOf<
      [args?: string | undefined]
    >();

    const withArgs = queries.getUser('test-id');
    expectTypeOf<Parameters<typeof withArgs.toQuery>>().toEqualTypeOf<
      [{userId: string}]
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

    expectTypeOf<ContextTypeOfCustomQueries<typeof queries>>().toEqualTypeOf<
      {userId: string} & {role: 'admin' | 'user'}
    >();

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
