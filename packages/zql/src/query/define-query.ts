import type {StandardSchemaV1} from '@standard-schema/spec';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {asQueryInternals} from './query-internals.ts';
import type {AnyQuery, Query} from './query.ts';
import {validateInput} from './validate-input.ts';

const defineQueryTag = Symbol();

export type DefineQueryOptions<Output, Input> = {
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
> = (options: {args: TArgs; ctx: TContext}) => Query<TSchema, TTable, TReturn>;

export type NamedQueryFunction<
  TName extends string,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  _TContext,
  TOutput extends ReadonlyJSONValue | undefined,
  TInput extends TOutput,
> = ([TOutput] extends [undefined]
  ? (() => Query<TSchema, TTable, TReturn>) &
      ((args: undefined) => Query<TSchema, TTable, TReturn>)
  : undefined extends TOutput
    ? (args?: TInput) => Query<TSchema, TTable, TReturn>
    : (args: TInput) => Query<TSchema, TTable, TReturn>) & {
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

export type DefinedQueryFunction<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TOutput extends ReadonlyJSONValue | undefined,
  TInput extends TOutput,
> = DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput> & {
  [defineQueryTag]: true;
  validator: StandardSchemaV1<TInput, TOutput> | undefined;
};

export function isDefinedQueryFunction<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TOutput extends ReadonlyJSONValue | undefined,
  TInput extends TOutput,
>(
  f: unknown,
): f is DefinedQueryFunction<
  TSchema,
  TTable,
  TReturn,
  TContext,
  TOutput,
  TInput
> {
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
  queryFn: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TArgs>,
): DefinedQueryFunction<TSchema, TTable, TReturn, TContext, TArgs, TArgs>;

// Overload for validator parameter - Input and Output can be different
export function defineQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TOutput extends ReadonlyJSONValue | undefined,
  TInput extends TOutput,
>(
  validator: StandardSchemaV1<TInput, TOutput>,
  queryFn: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>,
): DefinedQueryFunction<TSchema, TTable, TReturn, TContext, TOutput, TInput>;

// Implementation
export function defineQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TOutput extends ReadonlyJSONValue | undefined,
  TInput extends TOutput,
>(
  validatorOrQueryFn:
    | StandardSchemaV1<TInput, TOutput>
    | DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>,
  queryFn?: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>,
): DefinedQueryFunction<TSchema, TTable, TReturn, TContext, TOutput, TInput> {
  // Handle different parameter patterns
  let validator: StandardSchemaV1<TInput, TOutput> | undefined;
  let actualQueryFn: DefineQueryFunc<
    TSchema,
    TTable,
    TReturn,
    TContext,
    TOutput
  >;

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
  const f = actualQueryFn as DefinedQueryFunction<
    TSchema,
    TTable,
    TReturn,
    TContext,
    TOutput,
    TInput
  >;

  f[defineQueryTag] = true;
  f.validator = validator;
  return f;
}

/**
 * Wraps a defined query function with a query name, creating a function that
 * returns a Query that has bound the name and args to the instance.
 *
 * @param queryName - The name to assign to the query
 * @param definedQueryFunc - The defined query function to wrap
 * @returns A function that takes args and returns a Query
 */
export function wrapCustomQuery<TArgs, Context>(
  queryName: string,
  // oxlint-disable-next-line no-explicit-any
  definedQueryFunc: DefinedQueryFunction<any, any, any, any, any, any>,
  contextHolder: {context: Context},
): (args: TArgs) => AnyQuery {
  const {validator} = definedQueryFunc;
  return (args?: TArgs) => {
    // The args that we send to the server is the args that the user passed in.
    // This is what gets fed into the validator.
    let runtimeArgs = args;
    if (validator) {
      runtimeArgs = validateInput(queryName, args, validator, 'query');
    }
    const q = definedQueryFunc({
      args: runtimeArgs,
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
    queryFn: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TArgs>,
  ): DefinedQueryFunction<TSchema, TTable, TReturn, TContext, TArgs, TArgs>;

  <
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TOutput extends ReadonlyJSONValue | undefined,
    TInput extends TOutput,
  >(
    validator: StandardSchemaV1<TInput, TOutput>,
    queryFn: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>,
  ): DefinedQueryFunction<TSchema, TTable, TReturn, TContext, TOutput, TInput>;
} {
  return defineQuery as {
    <
      TSchema extends Schema,
      TTable extends keyof TSchema['tables'] & string,
      TReturn,
      TArgs extends ReadonlyJSONValue | undefined,
    >(
      queryFn: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TArgs>,
    ): DefinedQueryFunction<TSchema, TTable, TReturn, TContext, TArgs, TArgs>;

    <
      TSchema extends Schema,
      TTable extends keyof TSchema['tables'] & string,
      TReturn,
      TOutput extends ReadonlyJSONValue | undefined,
      TInput extends TOutput,
    >(
      validator: StandardSchemaV1<TInput, TOutput>,
      queryFn: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>,
    ): DefinedQueryFunction<
      TSchema,
      TTable,
      TReturn,
      TContext,
      TOutput,
      TInput
    >;
  };
}
