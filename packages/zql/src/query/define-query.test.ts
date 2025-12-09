import type {StandardSchemaV1} from '@standard-schema/spec';
import {describe, expect, expectTypeOf, test} from 'vitest';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {createBuilder} from './create-builder.ts';
import {
  defineQuery,
  defineQueryWithType,
  isQueryDefinition,
} from './define-query.ts';
import {asQueryInternals} from './query-internals.ts';
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

    const result = query.fn({args: undefined, ctx: 'noOptionsContext'});
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

    const result = query.fn({args: 123, ctx: 'validatorContext'});
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

    const result = query.fn({
      args: undefined,
      ctx: 'undefinedValidatorContext',
    });
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
    const result = query.fn({args: 'input', ctx: 'asyncContext'});
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
    const result1 = query1.fn({args: undefined, ctx: 'ctx1'});
    const result2 = query2.fn({args: 'test', ctx: 'ctx2'});

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
    expectTypeOf<Parameters<typeof query.fn>>().toEqualTypeOf<
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
    expectTypeOf<Parameters<typeof query.fn>>().toEqualTypeOf<
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

    expectTypeOf<Parameters<typeof query.fn>>().toEqualTypeOf<
      [{args: string; ctx: string}]
    >();
  });

  test('undefined args handling', () => {
    const query = defineQuery(({args}: {args: undefined}) => {
      expectTypeOf(args).toEqualTypeOf<undefined>();
      return builder.foo;
    });

    // Should be callable with args: undefined
    expectTypeOf<Parameters<typeof query.fn>>().toEqualTypeOf<
      [{args: undefined; ctx: unknown}]
    >();
  });

  test('QueryDefinition phantom tracks table/input/output/return/context', () => {
    type Ctx = {tenant: string};
    const validator = makeValidator<{raw: string}, {parsed: number}>(data => {
      const parsed = parseInt((data as {raw: string}).raw, 10);
      if (Number.isNaN(parsed)) {
        throw new Error('bad');
      }
      return {parsed};
    });

    const qDef = defineQuery(
      validator,
      ({args, ctx: _ctx}: {args: {parsed: number}; ctx: Ctx}) =>
        builder.foo.where('val', '=', args.parsed),
    );

    expectTypeOf(qDef['~']['$tableName']).toEqualTypeOf<'foo'>();
    expectTypeOf(qDef['~']['$input']).toEqualTypeOf<{raw: string}>();
    expectTypeOf(qDef['~']['$output']).toEqualTypeOf<{parsed: number}>();
    expectTypeOf(qDef['~']['$return']).toEqualTypeOf<
      PullRow<'foo', typeof schema>
    >();
    expectTypeOf(qDef['~']['$context']).toEqualTypeOf<Ctx>();
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
    expectTypeOf<Parameters<typeof validatedQuery.fn>>().toEqualTypeOf<
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
    expectTypeOf<Parameters<typeof query1.fn>>().toEqualTypeOf<
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
    expectTypeOf<Parameters<typeof query2.fn>>().toEqualTypeOf<
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
    expectTypeOf<Parameters<typeof query3.fn>>().toEqualTypeOf<
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
    expectTypeOf<Parameters<typeof query4.fn>>().toEqualTypeOf<
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

    expectTypeOf<typeof query.fn>().returns.toEqualTypeOf<
      Query<'foo', Schema, PullRow<'foo', typeof schema>>
    >();
    expectTypeOf<typeof query.fn>()
      .parameter(0)
      .toEqualTypeOf<{args: {id: string}; ctx: Context}>();
  });

  test('defineQuery infers args and context types', () => {
    const query = defineQuery(({args, ctx}) => {
      expectTypeOf(args).toEqualTypeOf<ReadonlyJSONValue | undefined>();
      expectTypeOf(ctx).toEqualTypeOf<unknown>();
      return builder.foo.where('id', '=', 'test');
    });

    expectTypeOf<typeof query.fn>().returns.toEqualTypeOf<
      Query<'foo', Schema, PullRow<'foo', typeof schema>>
    >();
    expectTypeOf<typeof query.fn>()
      .parameter(0)
      .toEqualTypeOf<{args: ReadonlyJSONValue | undefined; ctx: unknown}>();
  });
});

describe('defineQueryWithType', () => {
  test('binds types correctly', () => {
    type Context = {userId: string; role: 'admin' | 'user'};

    const defineQueryTyped = defineQueryWithType<typeof schema, Context>();

    const q = defineQueryTyped<
      {id: string},
      PullRow<'foo', typeof schema>,
      'foo'
    >(({args, ctx}) => {
      expectTypeOf(args).toEqualTypeOf<{id: string}>();
      expectTypeOf(ctx).toEqualTypeOf<Context>();
      return builder.foo.where('id', '=', args.id);
    });

    expectTypeOf<(typeof q)['~']['$context']>().toEqualTypeOf<Context>();
    expectTypeOf<(typeof q)['~']['$return']>().toEqualTypeOf<
      PullRow<'foo', typeof schema>
    >();
  });

  test('works with validator', () => {
    type Context = {userId: string};
    const define = defineQueryWithType<typeof schema, Context>();

    const q = define(
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

    const result = query.fn({args: undefined, ctx: {tenant: 't1'}});
    expect(asQueryInternals(result).ast).toEqual({table: 'foo'});
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
