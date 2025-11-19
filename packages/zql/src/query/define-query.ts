import type {StandardSchemaV1} from '@standard-schema/spec';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {asQueryInternals} from './query-internals.ts';
import type {AnyQuery, Query} from './query.ts';
import {validateInput} from './validate-input.ts';

const defineQueryTag = Symbol();

/**
 * A query definition function that has been wrapped by `defineQuery`.
 * Contains the original function plus metadata (validator and tag).
 */
export type QueryDefinition<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
> = ((options: {
  args: TOutput;
  ctx: TContext;
}) => Query<TSchema, TTable, TReturn>) & {
  [defineQueryTag]: true;
  validator: StandardSchemaV1<TInput, TOutput> | undefined;
};

export function isQueryDefinition<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
>(
  f: unknown,
): f is QueryDefinition<TSchema, TTable, TReturn, TContext, TInput, TOutput> {
  // oxlint-disable-next-line no-explicit-any
  return typeof f === 'function' && (f as any)[defineQueryTag];
}

// Overload for no validator parameter with default inference for untyped functions
export function defineQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TArgs extends ReadonlyJSONValue | undefined,
>(
  queryFn: (options: {
    args: TArgs;
    ctx: TContext;
  }) => Query<TSchema, TTable, TReturn>,
): QueryDefinition<TSchema, TTable, TReturn, TContext, TArgs, TArgs>;

// Overload for validator parameter - Input and Output can be different
export function defineQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
>(
  validator: StandardSchemaV1<TInput, TOutput>,
  queryFn: (options: {
    args: TOutput;
    ctx: TContext;
  }) => Query<TSchema, TTable, TReturn>,
): QueryDefinition<TSchema, TTable, TReturn, TContext, TInput, TOutput>;

// Implementation
export function defineQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
>(
  validatorOrQueryFn:
    | StandardSchemaV1<TInput, TOutput>
    | ((options: {
        args: TOutput;
        ctx: TContext;
      }) => Query<TSchema, TTable, TReturn>),
  queryFn?: (options: {
    args: TOutput;
    ctx: TContext;
  }) => Query<TSchema, TTable, TReturn>,
): QueryDefinition<TSchema, TTable, TReturn, TContext, TInput, TOutput> {
  // Handle different parameter patterns
  let validator: StandardSchemaV1<TInput, TOutput> | undefined;
  let actualQueryFn: (options: {
    args: TOutput;
    ctx: TContext;
  }) => Query<TSchema, TTable, TReturn>;

  if (typeof validatorOrQueryFn === 'function') {
    // defineQuery(queryFn) - no validator
    validator = undefined;
    actualQueryFn = validatorOrQueryFn;
  } else {
    // defineQuery(validator, queryFn) - with validator
    validator = validatorOrQueryFn;
    actualQueryFn = must(queryFn);
  }

  // Pass through the function as-is, only adding tag and validator
  const f = actualQueryFn as QueryDefinition<
    TSchema,
    TTable,
    TReturn,
    TContext,
    TInput,
    TOutput
  >;

  f[defineQueryTag] = true;
  f.validator = validator;
  return f;
}

/**
 * Wraps a query definition with a query name and context, creating a function that
 * returns a Query with the name and args bound to the instance.
 *
 * @param queryName - The name to assign to the query
 * @param f - The query definition to wrap
 * @param contextHolder - An object containing the context to pass to the query
 * @returns A function that takes args and returns a Query
 */
export function wrapCustomQuery<TArgs, Context>(
  queryName: string,
  // oxlint-disable-next-line no-explicit-any
  f: QueryDefinition<any, any, any, any, any, any>,
  contextHolder: {context: Context},
): (args: TArgs) => AnyQuery {
  const {validator} = f;
  const validate = validator
    ? (args: TArgs) =>
        validateInput<TArgs, TArgs>(queryName, args, validator, 'query')
    : (args: TArgs) => args;

  return (args?: TArgs) => {
    // The args that we send to the server is the args that the user passed in.
    // This is what gets fed into the validator.
    const q = f({
      args: validate(args as TArgs),
      ctx: contextHolder.context,
    });
    return asQueryInternals(q).nameAndArgs(
      queryName,
      // TODO(arv): Get rid of the array?
      args === undefined ? [] : [args as unknown as ReadonlyJSONValue],
    );
  };
}

/**
 * Creates a type-safe query definition function that is parameterized by a
 * custom context type, without requiring a query name.
 *
 * This utility allows you to define queries with explicit context typing,
 * ensuring that the query function receives the correct context type. It
 * returns a function that can be used to define queries with schema,
 * table, input, and output types.
 *
 * @typeParam TContext - The type of the context object that will be passed to
 * the query function.
 *
 * @returns A function for defining queries with the specified context type.
 *
 * @example
 * ```ts
 * const defineQuery2 = defineQuery2WithContextType<MyContext>();
 * const myQuery = defineQuery2(
 *   z.string(),
 *   ({ctx, args}) => {
 *     ctx satisfies MyContext;
 *     ...
 *   },
 * );
 * ```
 */
export function defineQueryWithContextType<TContext>(): {
  <
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TArgs extends ReadonlyJSONValue | undefined,
  >(
    queryFn: (options: {
      args: TArgs;
      ctx: TContext;
    }) => Query<TSchema, TTable, TReturn>,
  ): QueryDefinition<TSchema, TTable, TReturn, TContext, TArgs, TArgs>;

  <
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TInput extends ReadonlyJSONValue | undefined,
    TOutput extends ReadonlyJSONValue | undefined,
  >(
    validator: StandardSchemaV1<TInput, TOutput>,
    queryFn: (options: {
      args: TOutput;
      ctx: TContext;
    }) => Query<TSchema, TTable, TReturn>,
  ): QueryDefinition<TSchema, TTable, TReturn, TContext, TInput, TOutput>;
} {
  return defineQuery as {
    <
      TSchema extends Schema,
      TTable extends keyof TSchema['tables'] & string,
      TReturn,
      TArgs extends ReadonlyJSONValue | undefined,
    >(
      queryFn: (options: {
        args: TArgs;
        ctx: TContext;
      }) => Query<TSchema, TTable, TReturn>,
    ): QueryDefinition<TSchema, TTable, TReturn, TContext, TArgs, TArgs>;

    <
      TSchema extends Schema,
      TTable extends keyof TSchema['tables'] & string,
      TReturn,
      TInput extends ReadonlyJSONValue | undefined,
      TOutput extends ReadonlyJSONValue | undefined,
    >(
      validator: StandardSchemaV1<TInput, TOutput>,
      queryFn: (options: {
        args: TOutput;
        ctx: TContext;
      }) => Query<TSchema, TTable, TReturn>,
    ): QueryDefinition<TSchema, TTable, TReturn, TContext, TInput, TOutput>;
  };
}
