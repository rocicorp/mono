import type {StandardSchemaV1} from '@standard-schema/spec';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {Query} from './query.ts';
import {RootNamedQuery} from './root-named-query.ts';

export type DefineQueryOptions<Input, Output> = {
  validator?: StandardSchemaV1<Input, Output> | undefined;
};

/**
 * Function type for root query functions that take context and args.
 */
export type DefineQueryFunc<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TArgs,
> = (options: {
  ctx: TContext;
  args: TArgs;
}) => Query<TSchema, TTable, TReturn, TContext>;

export type NamedQueryFunction<
  TName extends string,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TOutput extends ReadonlyJSONValue | undefined,
  TInput extends ReadonlyJSONValue | undefined,
> = ([TOutput] extends [undefined]
  ? (() => Query<TSchema, TTable, TReturn, TContext>) &
      ((args: undefined) => Query<TSchema, TTable, TReturn, TContext>)
  : undefined extends TOutput
    ? (args?: TInput) => Query<TSchema, TTable, TReturn, TContext>
    : (args: TInput) => Query<TSchema, TTable, TReturn, TContext>) & {
  queryName: TName;
};

export type AnyNamedQueryFunction = NamedQueryFunction<
  string,
  Schema,
  string,
  // oxlint-disable-next-line no-explicit-any
  any,
  // oxlint-disable-next-line no-explicit-any
  any,
  ReadonlyJSONValue | undefined,
  ReadonlyJSONValue | undefined
>;

// Overload for no options parameter with default inference for untyped functions
export function defineQuery<
  TName extends string,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TArgs extends ReadonlyJSONValue | undefined,
>(
  name: TName,
  queryFn: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TArgs>,
): NamedQueryFunction<TName, TSchema, TTable, TReturn, TContext, TArgs, TArgs>;

// Overload for options parameter with validator - Input and Output can be different
export function defineQuery<
  TName extends string,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TOutput extends ReadonlyJSONValue | undefined,
  TInput extends ReadonlyJSONValue | undefined = TOutput,
>(
  name: TName,
  options: DefineQueryOptions<TInput, TOutput>,
  queryFn: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>,
): NamedQueryFunction<
  TName,
  TSchema,
  TTable,
  TReturn,
  TContext,
  TOutput,
  TInput
>;

// Overload for options parameter without validator with default inference
export function defineQuery<
  TName extends string,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TArgs extends ReadonlyJSONValue | undefined,
>(
  name: TName,
  options: {},
  queryFn: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TArgs>,
): NamedQueryFunction<TName, TSchema, TTable, TReturn, TContext, TArgs, TArgs>;

// Implementation
export function defineQuery<
  TName extends string,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TOutput extends ReadonlyJSONValue | undefined,
  TInput extends ReadonlyJSONValue | undefined = TOutput,
>(
  name: TName,
  optionsOrQueryFn:
    | DefineQueryOptions<TInput, TOutput>
    | DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>,
  queryFn?: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>,
): NamedQueryFunction<
  TName,
  TSchema,
  TTable,
  TReturn,
  TContext,
  TOutput,
  TInput
> {
  // Handle different parameter patterns
  let defineOptions: DefineQueryOptions<TInput, TOutput> | undefined;
  let actualQueryFn: DefineQueryFunc<
    TSchema,
    TTable,
    TReturn,
    TContext,
    TOutput
  >;

  if (typeof optionsOrQueryFn === 'function') {
    // defineQuery(name, queryFn) - no options
    defineOptions = undefined;
    actualQueryFn = optionsOrQueryFn;
  } else {
    // defineQuery(name, options, queryFn) - with options
    defineOptions = optionsOrQueryFn;
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    actualQueryFn = queryFn!;
  }

  const f = ((args?: TInput) =>
    new RootNamedQuery(
      name,
      actualQueryFn,
      args,
      defineOptions?.validator,
    )) as unknown as NamedQueryFunction<
    TName,
    TSchema,
    TTable,
    TReturn,
    TContext,
    TOutput,
    TInput
  >;
  f.queryName = name;
  return f;
}

/**
 * Creates a type-safe query definition function that is parameterized by a
 * custom context type.
 *
 * This utility allows you to define queries with explicit context typing,
 * ensuring that the query function receives the correct context type. It
 * returns a function that can be used to define named queries with schema,
 * table, input, and output types.
 *
 * @typeParam TContext - The type of the context object that will be passed to
 * the query function.
 *
 * @returns A function for defining named queries with the specified context
 * type.
 *
 * @example
 * ```ts
 * const defineQuery = defineQueryWithContextType<MyContext>();
 * const myQuery = defineQuery(
 *   "getUser",
 *   {validator: z.string()},
 *   ({ctx, args}) => {
 *     ctx satisfies MyContext;
 *     ...
 *   },
 * );
 * ```
 */
export function defineQueryWithContextType<TContext>(): <
  TName extends string,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TOutput extends ReadonlyJSONValue | undefined,
  TInput extends ReadonlyJSONValue | undefined = TOutput,
>(
  name: TName,
  optionsOrQueryFn:
    | DefineQueryOptions<TInput, TOutput>
    | DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>,
  queryFn?: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>,
) => NamedQueryFunction<
  TName,
  TSchema,
  TTable,
  TReturn,
  TContext,
  TOutput,
  TInput
> {
  return defineQuery as <
    TName extends string,
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TOutput extends ReadonlyJSONValue | undefined,
    TInput extends ReadonlyJSONValue | undefined = TOutput,
  >(
    name: TName,
    optionsOrQueryFn:
      | DefineQueryOptions<TInput, TOutput>
      | DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>,
    queryFn?: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>,
  ) => NamedQueryFunction<
    TName,
    TSchema,
    TTable,
    TReturn,
    TContext,
    TOutput,
    TInput
  >;
}
