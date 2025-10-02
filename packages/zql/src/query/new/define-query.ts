import type {StandardSchemaV1} from '@standard-schema/spec';
import type {ReadonlyJSONValue} from '../../../../shared/src/json.ts';
import type {Schema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {RootNamedQuery} from './root-named-query.ts';
import type {Func} from './types.ts';

export type DefineQueryOptions<Input, Output> = {
  validator?: StandardSchemaV1<Input, Output> | undefined;
};

export type NamedQueryFunction<
  TName extends string,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TOutput extends ReadonlyJSONValue | undefined,
  TInput extends ReadonlyJSONValue | undefined,
> = ([TOutput] extends [undefined]
  ? (() => RootNamedQuery<
      TName,
      TSchema,
      TTable,
      TReturn,
      TContext,
      TOutput,
      TInput
    >) &
      ((
        args: undefined,
      ) => RootNamedQuery<
        TName,
        TSchema,
        TTable,
        TReturn,
        TContext,
        TOutput,
        TInput
      >)
  : undefined extends TOutput
    ? (
        args?: TInput,
      ) => RootNamedQuery<
        TName,
        TSchema,
        TTable,
        TReturn,
        TContext,
        TOutput,
        TInput
      >
    : (
        args: TInput,
      ) => RootNamedQuery<
        TName,
        TSchema,
        TTable,
        TReturn,
        TContext,
        TOutput,
        TInput
      >) & {queryName: TName};

// Overload for no options parameter with default inference for untyped functions
export function defineQuery<
  TName extends string,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TArgs extends ReadonlyJSONValue | undefined,
>(
  name1: TName,
  queryFn: Func<TSchema, TTable, TReturn, TContext, TArgs>,
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
  name2: TName,
  options: DefineQueryOptions<TInput, TOutput>,
  queryFn: Func<TSchema, TTable, TReturn, TContext, TOutput>,
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
  name3: TName,
  options: {},
  queryFn: Func<TSchema, TTable, TReturn, TContext, TArgs>,
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
    | Func<TSchema, TTable, TReturn, TContext, TOutput>,
  queryFn?: Func<TSchema, TTable, TReturn, TContext, TOutput>,
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
  let actualQueryFn: Func<TSchema, TTable, TReturn, TContext, TOutput>;

  if (typeof optionsOrQueryFn === 'function') {
    // defineQuery(name, queryFn) - no options
    defineOptions = undefined;
    actualQueryFn = optionsOrQueryFn;
  } else {
    // defineQuery(name, options, queryFn) - with options
    defineOptions = optionsOrQueryFn;
    actualQueryFn = queryFn!;
  }

  const f = ((args?: TInput) =>
    new RootNamedQuery(
      name,
      actualQueryFn,
      args,
      defineOptions?.validator,
    )) as NamedQueryFunction<
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
