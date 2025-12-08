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
import type {PullRow, Query} from './query-builder.ts';
import {asQueryInternals} from './query-internals.ts';
import {
  createQuery,
  defineQueries,
  defineQueriesWithType,
  getQuery,
  isQueryRegistry,
  mustGetQuery,
} from './query-registry.ts';
import {
  addContextToQuery,
  defineQuery,
  defineQueryWithType,
  type CustomQuery,
  type QueryRequest,
} from './query.ts';

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

describe('createQuery', () => {
  test('validator failure throws before executing query', () => {
    const validator: StandardSchemaV1<string, string> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: () => ({issues: [{message: 'bad'}]}),
      },
    };

    const run = vi.fn();

    const queryDef = defineQuery(validator, ({args}) => {
      run(args);
      return builder.foo.where('id', '=', args);
    });

    const query = createQuery('test', queryDef);

    expect(() =>
      addContextToQuery(query('boom'), {}),
    ).toThrowErrorMatchingInlineSnapshot(
      `[Error: Validation failed for query test: bad]`,
    );
    expect(run).not.toHaveBeenCalled();
  });
});

describe('defineQueries', () => {
  test('should support (args).toZQL(ctx)', () => {
    const queries = defineQueries({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    const result = addContextToQuery(queries.getUser('test-id'), {
      userId: 'ctx-user',
    });
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

    const result = addContextToQuery(queries.users.getById('nested-id'), {
      userId: 'ctx-user',
    });

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

    const result = addContextToQuery(queries.getUser('test-id'), {
      userId: 'ctx-user',
    });
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

  test('should allow calling args multiple times', () => {
    const queries = defineQueries({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    const first = queries.getUser('first');
    const second = queries.getUser('second');

    expect(
      asQueryInternals(addContextToQuery(first, {userId: 'ctx'})).ast,
    ).toEqual({
      table: 'foo',
      where: {
        left: {name: 'id', type: 'column'},
        op: '=',
        right: {type: 'literal', value: 'first'},
        type: 'simple',
      },
    });

    expect(
      asQueryInternals(addContextToQuery(second, {userId: 'ctx'})).ast,
    ).toEqual({
      table: 'foo',
      where: {
        left: {name: 'id', type: 'column'},
        op: '=',
        right: {type: 'literal', value: 'second'},
        type: 'simple',
      },
    });
  });

  test('query callable should not expose toZQL without args', () => {
    const queries = defineQueries({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    // oxlint-disable-next-line no-explicit-any -- testing runtime shape
    const q = queries.getUser as any;
    expect(q.toZQL).toBeUndefined();
  });

  test('should allow optional args when Args type is undefined', () => {
    const queries = defineQueries({
      getAllUsers: defineQuery(
        ({ctx: _ctx}: {args: undefined; ctx: {userId: string}}) => builder.foo,
      ),
    });

    // Should work without calling with args
    const result1 = addContextToQuery(queries.getAllUsers(), {
      userId: 'ctx-user',
    });
    expect(asQueryInternals(result1).ast).toEqual({
      table: 'foo',
    });

    // Should also work with explicit undefined
    const result2 = addContextToQuery(queries.getAllUsers(undefined), {
      userId: 'ctx-user',
    });
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
    const result = addContextToQuery(queries.getByVal('42'), {});

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
    const result = addContextToQuery(queries.getValue(undefined), {});

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
    const result2 = queries.getValue.fn({args: 'explicit', ctx: {}});

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

    expectTypeOf<typeof queries.getFoo>().toExtend<
      CustomQuery<
        'foo',
        {id: string},
        {id: string},
        typeof schema,
        PullRow<'foo', typeof schema>,
        Context1
      >
    >();
    expectTypeOf<typeof queries.getFoo>()
      .parameter(0)
      .toEqualTypeOf<{id: string}>();
    expectTypeOf<
      (typeof queries.getFoo)['~']['$context']
    >().toEqualTypeOf<Context1>();

    expectTypeOf<typeof queries.getBar>().returns.toExtend<
      QueryRequest<
        'bar',
        {some: 'baz'},
        typeof schema,
        PullRow<'bar', typeof schema>,
        Context2
      >
    >();
    expectTypeOf<typeof queries.getBar>()
      .parameter(0)
      .toEqualTypeOf<{some: 'baz'}>();
    expectTypeOf<
      (typeof queries.getBar)['~']['$context']
    >().toEqualTypeOf<Context2>();
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
  test('defineQueries keeps validator input for callable and context for toZQL', () => {
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
    expectTypeOf<(typeof withArgs)['~']['$context']>().toEqualTypeOf<Context>();
  });

  test('initial Query should have args callable but no toZQL', () => {
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
    // And retain types
    expectTypeOf(queries.getUser['~'].$context).toExtend<{userId: string}>();

    // Should NOT have context method
    expectTypeOf(queries.getUser).not.toHaveProperty('context');

    // Should NOT have toZQL method (args not set)
    expectTypeOf(queries.getUser).not.toHaveProperty('toZQL');
  });

  test('initial query should have fn that takes args and ctx', () => {
    const queries = defineQueriesWithType<typeof schema>()({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    expectTypeOf<Parameters<typeof queries.getUser.fn>>().toEqualTypeOf<
      [{args: string; ctx: {userId: string}}]
    >();
  });

  test('after setting args, should have toZQL but not be callable again', () => {
    const queries = defineQueriesWithType<typeof schema>()({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    const withArgs = queries.getUser('test-id');

    // Should have toZQL method that takes context
    expectTypeOf<(typeof withArgs)['~']['$context']>().toEqualTypeOf<{
      userId: string;
    }>();

    // Should NOT be callable - verify it's not a function type
    expectTypeOf<typeof withArgs>().not.toMatchTypeOf<(args: string) => any>();
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

  test('context type should be enforced on toZQL', () => {
    const queries = defineQueries({
      getUser: defineQuery(
        ({args, ctx: _ctx}: {args: string; ctx: {userId: string}}) =>
          builder.foo.where('id', '=', args),
      ),
    });

    const withArgs = queries.getUser('test-id');

    // Correct context type
    expectTypeOf<(typeof withArgs)['~']['$context']>().toEqualTypeOf<{
      userId: string;
    }>();
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

    // Context is unknown
    expectTypeOf(withArgs['~']['$context']).toBeUnknown();
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
    expectTypeOf<(typeof q1)['~']['$context']>().toEqualTypeOf<{
      userId: string;
    }>();
    // q2 only needs role
    expectTypeOf<(typeof q2)['~']['$context']>().toEqualTypeOf<{
      role: 'admin' | 'user';
    }>();
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
    const resultA = addContextToQuery(extended.queryA('test-a'), {
      userId: 'user1',
    });
    expect(asQueryInternals(resultA).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'test-a'},
      },
    });

    const resultC = addContextToQuery(extended.queryC('test-c'), {
      userId: 'user1',
    });
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
    const resultA = addContextToQuery(extended.queryA('test-a'), {
      userId: 'user1',
    });
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
    const resultB = addContextToQuery(extended.queryB('overridden'), {
      userId: 'user1',
    });
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
    const resultA = addContextToQuery(merged.queryA('test-a'), {
      userId: 'user1',
    });
    expect(asQueryInternals(resultA).ast).toEqual({
      table: 'foo',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 'test-a'},
      },
    });

    const resultC = addContextToQuery(merged.queryC('test-c'), {
      userId: 'user1',
    });
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
    const resultA = addContextToQuery(merged.queryA('test-a'), {
      userId: 'user1',
    });
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
    const resultB = addContextToQuery(merged.queryB('overridden'), {
      userId: 'user1',
    });
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
    expectTypeOf<(typeof extended.queryB)['~']['$context']>().toEqualTypeOf<{
      userId: string;
    }>();
  });

  test('toZQL context types should be preserved', () => {
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

    expectTypeOf<(typeof qA)['~']['$context']>().toEqualTypeOf<{
      userId: string;
    }>();
    expectTypeOf<(typeof qB)['~']['$context']>().toEqualTypeOf<{
      role: string;
    }>();
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

    const fooQuery = extended.getFoo().toZQL({});
    const barQuery = extended.getBar().toZQL({});

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
    expectTypeOf<(typeof merged.queryB)['~']['$context']>().toEqualTypeOf<{
      userId: string;
    }>();
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
    expectTypeOf<(typeof extended.users.byId)['~']['$context']>().toEqualTypeOf<
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
    expect(query1['~']).toEqual('Query');
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

    expectTypeOf(query2['~']['$tableName']).toEqualTypeOf<'foo' | 'bar'>();
    expectTypeOf(query2['~']['$schema']).toEqualTypeOf<Schema>();
    expectTypeOf(query2['~']['$input']).toEqualTypeOf<
      ReadonlyJSONValue | undefined
    >();
    expectTypeOf(query2['~']['$context']).toEqualTypeOf<unknown>();
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
  expectTypeOf(query1['~']['$args']).toEqualTypeOf<{id: string}>();
  expectTypeOf(query1['~']['$context']).toEqualTypeOf<Context1>();
  expectTypeOf(query1['~']['$return']).toEqualTypeOf<
    PullRow<'foo', typeof schema>
  >();

  const query2 = queries.def2({id: '123'});
  expectTypeOf(query2['~']['$args']).toEqualTypeOf<{id: string}>();
  expectTypeOf(query2['~']['$context']).toEqualTypeOf<Context2>();
  expectTypeOf(query2['~']['$return']).toEqualTypeOf<
    PullRow<'bar', typeof schema>
  >();

  expectTypeOf(queries.def3)
    .parameter(0)
    .toEqualTypeOf<ReadonlyJSONValue | undefined>();
  const query3 = queries.def3('whatever');
  expectTypeOf(query3['~']['$args']).toEqualTypeOf<
    ReadonlyJSONValue | undefined
  >();
  expectTypeOf(query3['~']['$context']).toEqualTypeOf<unknown>();

  const query4 = queries.def4('test-string');
  expectTypeOf(query4['~']['$args']).toEqualTypeOf<string>();
  expectTypeOf(query4['~']['$context']).toEqualTypeOf<Context3>();
});
