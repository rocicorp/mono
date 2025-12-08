import type {StandardSchemaV1} from '@standard-schema/spec';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  DefaultContext,
  DefaultSchema,
} from '../../../zero-types/src/default-types.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {PullRow, Query} from './query-builder.ts';

// ----------------------------------------------------------------------------
// defineQuery
// ----------------------------------------------------------------------------

export type QueryDefinitionTypes<
  TTable extends string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput,
  TReturn,
  TContext,
> = 'QueryDefinition' & {
  readonly $tableName: TTable;
  readonly $input: TInput;
  readonly $output: TOutput;
  readonly $return: TReturn;
  readonly $context: TContext;
};

/**
 * A query definition is the return type of `defineQuery()`.
 */
export type QueryDefinition<
  TTable extends string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TReturn,
  TContext = DefaultContext,
> = {
  readonly 'fn': QueryDefinitionFunction<TTable, TOutput, TReturn, TContext>;
  readonly 'validator': StandardSchemaV1<TInput, TOutput> | undefined;
  readonly '~': QueryDefinitionTypes<
    TTable,
    TInput,
    TOutput,
    TReturn,
    TContext
  >;
};

// oxlint-disable-next-line no-explicit-any
export type AnyQueryDefinition = QueryDefinition<any, any, any, any, any>;

export function isQueryDefinition(f: unknown): f is AnyQueryDefinition {
  return (
    typeof f === 'object' &&
    f !== null &&
    (f as {['~']?: unknown})['~'] === 'QueryDefinition'
  );
}

/**
 * Defines a query to be used with {@link defineQueries}.
 *
 * The query function receives an object with `args` (the query arguments) and
 * `ctx` (the context). It should return a {@link Query} built using a builder
 * created from {@link createBuilder}.
 *
 * Note: A query defined with `defineQuery` must be passed to
 * {@link defineQueries} to be usable. The query name is derived from its
 * position in the `defineQueries` object.
 *
 * @example
 * ```ts
 * const builder = createBuilder(schema);
 *
 * const queries = defineQueries({
 *   // Simple query with no arguments
 *   allIssues: defineQuery(() => builder.issue.orderBy('created', 'desc')),
 *
 *   // Query with typed arguments
 *   issueById: defineQuery(({args}: {args: {id: string}}) =>
 *     builder.issue.where('id', args.id).one(),
 *   ),
 *
 *   // Query with validation using a Standard Schema validator (e.g., Zod)
 *   issuesByStatus: defineQuery(
 *     z.object({status: z.enum(['open', 'closed'])}),
 *     ({args}) => builder.issue.where('status', args.status),
 *   ),
 *
 *   // Query using context
 *   myIssues: defineQuery(({ctx}: {ctx: {userID: string}}) =>
 *     builder.issue.where('creatorID', ctx.userID),
 *   ),
 * });
 * ```
 *
 * @param queryFn - A function that receives `{args, ctx}` and returns a Query.
 * @returns A {@link QueryDefinition} that can be passed to {@link defineQueries}.
 *
 * @overload
 * @param validator - A Standard Schema validator for the arguments.
 * @param queryFn - A function that receives `{args, ctx}` and returns a Query.
 * @returns A {@link QueryDefinition} with validated arguments.
 */
// Overload for no validator parameter with default inference for untyped functions
export function defineQuery<
  TInput extends ReadonlyJSONValue | undefined,
  TContext = DefaultContext,
  TSchema extends Schema = DefaultSchema,
  TTable extends keyof TSchema['tables'] & string = keyof TSchema['tables'] &
    string,
  TReturn = PullRow<TTable, TSchema>,
>(
  queryFn: QueryDefinitionFunction<TTable, TInput, TReturn, TContext>,
): QueryDefinition<TTable, TInput, TInput, TReturn, TContext>;

// Overload for validator parameter - Input and Output can be different
export function defineQuery<
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TContext = DefaultContext,
  TSchema extends Schema = DefaultSchema,
  TTable extends keyof TSchema['tables'] & string = keyof TSchema['tables'] &
    string,
  TReturn = PullRow<TTable, TSchema>,
>(
  validator: StandardSchemaV1<TInput, TOutput>,
  queryFn: QueryDefinitionFunction<TTable, TOutput, TReturn, TContext>,
): QueryDefinition<TTable, TInput, TOutput, TReturn, TContext>;

// Implementation
export function defineQuery<
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TContext = DefaultContext,
  TSchema extends Schema = DefaultSchema,
  TTable extends keyof TSchema['tables'] & string = keyof TSchema['tables'] &
    string,
  TReturn = PullRow<TTable, TSchema>,
>(
  validatorOrQueryFn:
    | StandardSchemaV1<TInput, TOutput>
    | QueryDefinitionFunction<TTable, TOutput, TReturn, TContext>,
  queryFn?: QueryDefinitionFunction<TTable, TOutput, TReturn, TContext>,
): QueryDefinition<TTable, TInput, TOutput, TReturn, TContext> {
  // Handle different parameter patterns
  let validator: StandardSchemaV1<TInput, TOutput> | undefined;
  let actualQueryFn: QueryDefinitionFunction<
    TTable,
    TOutput,
    TReturn,
    TContext
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

  const queryDefinition: QueryDefinition<
    TTable,
    TInput,
    TOutput,
    TReturn,
    TContext
  > = {
    'fn': actualQueryFn,
    'validator': validator,
    '~': 'QueryDefinition' as unknown as QueryDefinitionTypes<
      TTable,
      TInput,
      TOutput,
      TReturn,
      TContext
    >,
  };
  return queryDefinition;
}

/**
 * Returns a typed version of {@link defineQuery} with the schema and context
 * types pre-specified. This enables better type inference when defining
 * queries.
 *
 * @example
 * ```ts
 * const zql = createBuilder(schema);
 *
 * // With both Schema and Context types
 * const defineAppQuery = defineQueryWithType<AppSchema, AppContext>();
 * const myQuery = defineAppQuery(({ctx}) =>
 *   zql.issue.where('userID', ctx.userID),
 * );
 *
 * // With just Context type (Schema inferred)
 * const defineAppQuery = defineQueryWithType<AppContext>();
 * ```
 *
 * @typeParam S - The Zero schema type.
 * @typeParam C - The context type passed to query functions.
 * @returns A function equivalent to {@link defineQuery} but with types
 *   pre-bound.
 */
export function defineQueryWithType<
  S extends Schema,
  C = unknown,
>(): TypedDefineQuery<S, C>;

/**
 * Returns a typed version of {@link defineQuery} with the context type
 * pre-specified.
 *
 * @typeParam C - The context type passed to query functions.
 * @returns A function equivalent to {@link defineQuery} but with the context
 *   type pre-bound.
 */
export function defineQueryWithType<C>(): TypedDefineQuery<Schema, C>;

export function defineQueryWithType() {
  return defineQuery;
}

/**
 * The return type of defineQueryWithType. A function matching the
 * defineQuery overloads but with Schema and Context pre-bound.
 */
type TypedDefineQuery<TSchema extends Schema, TContext> = {
  // Without validator
  <
    TArgs extends ReadonlyJSONValue | undefined,
    TReturn,
    TTable extends keyof TSchema['tables'] & string = keyof TSchema['tables'] &
      string,
  >(
    queryFn: QueryDefinitionFunction<TTable, TArgs, TReturn, TContext>,
  ): QueryDefinition<TTable, TArgs, TArgs, TReturn, TContext>;

  // With validator
  <
    TInput extends ReadonlyJSONValue | undefined,
    TOutput extends ReadonlyJSONValue | undefined,
    TReturn,
    TTable extends keyof TSchema['tables'] & string = keyof TSchema['tables'] &
      string,
  >(
    validator: StandardSchemaV1<TInput, TOutput>,
    queryFn: QueryDefinitionFunction<TTable, TOutput, TReturn, TContext>,
  ): QueryDefinition<TTable, TInput, TOutput, TReturn, TContext>;
};

export type QueryDefinitionFunction<
  TTable extends string,
  TInput extends ReadonlyJSONValue | undefined,
  TReturn,
  TContext,
> = (options: {args: TInput; ctx: TContext}) => Query<TTable, Schema, TReturn>;

// ----------------------------------------------------------------------------
// CustomQuery and QueryRequest types
// ----------------------------------------------------------------------------

export type CustomQueryTypes<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TReturn,
  TContext,
> = 'Query' & {
  readonly $tableName: TTable;
  readonly $input: TInput;
  readonly $schema: TSchema;
  readonly $return: TReturn;
  readonly $context: TContext;
};

/**
 * CustomQuery is returned from defineQueries. It is a callable that captures
 * args and can be turned into a Query via {@link QueryRequest}.
 */
export type CustomQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined = TInput,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
> = {
  readonly 'queryName': string;
  readonly 'fn': QueryDefinitionFunction<TTable, TInput, TReturn, TContext>;
  readonly '~': CustomQueryTypes<TTable, TInput, TSchema, TReturn, TContext>;
} & CustomQueryCallable<TTable, TInput, TOutput, TSchema, TReturn, TContext>;

type CustomQueryCallable<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
> = [TInput] extends [undefined]
  ? () => QueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>
  : undefined extends TInput
    ? {
        (): QueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>;
        (
          args?: TInput,
        ): QueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>;
      }
    : {
        (
          args: TInput,
        ): QueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>;
      };

// oxlint-disable-next-line no-explicit-any
export type AnyCustomQuery = CustomQuery<string, any, any, Schema, any, any>;

export function isQuery(value: unknown): value is AnyCustomQuery {
  return (
    typeof value === 'function' &&
    typeof (value as {queryName?: unknown}).queryName === 'string' &&
    typeof (value as {fn?: unknown}).fn === 'function'
  );
}

export type QueryRequestTypes<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TReturn,
  TContext,
> = 'QueryRequest' & {
  readonly $tableName: TTable;
  readonly $input: TInput;
  readonly $output: TOutput;
  readonly $schema: TSchema;
  readonly $return: TReturn;
  readonly $context: TContext;
};

export type QueryRequest<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TReturn,
  TContext,
> = {
  readonly 'query': CustomQuery<
    TTable,
    TInput,
    TOutput,
    TSchema,
    TReturn,
    TContext
  >;
  readonly 'args': TInput;
  readonly '~': QueryRequestTypes<
    TTable,
    TInput,
    TOutput,
    TSchema,
    TReturn,
    TContext
  >;
};

/**
 * A shared type that can be a query request or a query builder.
 *
 * If it is a query request, it will be converted to a {@link Query} using the context.
 * Otherwise, it will be returned as is.
 */
export type QueryOrQueryRequest<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TReturn,
  TContext,
> =
  | QueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>
  | Query<TTable, TSchema, TReturn>;

/**
 * Converts a query request to a {@link Query} using the context,
 * or returns the query as is.
 *
 * @param query - The query request or query builder to convert
 * @param context - The context to use to convert the query request
 */
export const addContextToQuery = <
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TReturn,
  TContext,
>(
  query: QueryOrQueryRequest<
    TTable,
    TInput,
    TOutput,
    TSchema,
    TReturn,
    TContext
  >,
  context: TContext,
): Query<TTable, TSchema, TReturn> =>
  'query' in query ? query.query.fn({ctx: context, args: query.args}) : query;
