// oxlint-disable no-explicit-any
import type {StandardSchemaV1} from '@standard-schema/spec';
import type {Expand} from '../../../shared/src/expand.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  DefaultContext,
  DefaultSchema,
} from '../../../zero-types/src/default-types.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {PullRow, QueryBuilder} from './query-builder.ts';

/**
 * Query is returned from defineQueries. It is a callable that captures
 * args and can be turned into a QueryBuilder via {@link QueryRequest}.
 */
export type Query<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined = TInput,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
> = {
  /**
   * Type-only phantom property to surface query types in a covariant position.
   */
  '~': Expand<QueryTypes<TTable, TInput, TOutput, TSchema, TReturn, TContext>>;
  'queryName': string;
  'fn': (options: {
    args: TInput;
    ctx: TContext;
  }) => QueryBuilder<TTable, TSchema, TReturn>;
} & (undefined extends TInput
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
    });

export type AnyQuery = Query<string, any, any, Schema, any, any>;

export function isQuery(value: unknown): value is AnyQuery {
  return (
    typeof value === 'function' &&
    typeof (value as {queryName?: unknown}).queryName === 'string' &&
    typeof (value as {fn?: unknown}).fn === 'function'
  );
}

export type QueryTypes<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput,
  TSchema extends Schema,
  TReturn,
  TContext,
> = 'Query' & {
  readonly $tableName: TTable;
  readonly $input: TInput;
  readonly $output: TOutput;
  readonly $schema: TSchema;
  readonly $return: TReturn;
  readonly $context: TContext;
};

export type QueryRequestTypes<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput,
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

export type QueryRegistryTypes<TSchema extends Schema> = 'QueryRegistry' & {
  readonly $schema: TSchema;
};

export type QueryRequest<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TReturn,
  TContext,
> = {
  readonly 'args': TInput;
  readonly 'toZQL': (ctx: TContext) => QueryBuilder<TTable, TSchema, TReturn>;
  readonly '~': Expand<
    QueryRequestTypes<TTable, TInput, TOutput, TSchema, TReturn, TContext>
  >;
};

export type AnyQueryDefinition = QueryDefinition<any, any, any, any, any>;

type QueryDefinitionFunction<
  TTable extends string,
  TOutput extends ReadonlyJSONValue | undefined,
  TReturn,
  TContext,
> = (options: {
  args: TOutput;
  ctx: TContext;
}) => QueryBuilder<TTable, Schema, TReturn>;

/**
 * A query definition is the return type of `defineQuery()`.
 */
export type QueryDefinition<
  TTable extends string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TReturn,
  TContext = DefaultContext,
> = QueryDefinitionFunction<TTable, TOutput, TReturn, TContext> & {
  'validator': StandardSchemaV1<TInput, TOutput> | undefined;

  /**
   * Type-only phantom property to surface query types in a covariant position.
   */
  readonly '~': Expand<
    QueryDefinitionTypes<TTable, TInput, TOutput, TReturn, TContext>
  >;
};

export function isQueryDefinition(f: unknown): f is AnyQueryDefinition {
  return typeof f === 'function' && (f as any)['~'] === 'QueryDefinition';
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
  queryFn: (options: {
    args: TInput;
    ctx: TContext;
  }) => QueryBuilder<TTable, TSchema, TReturn>,
): QueryDefinition<TTable, TInput, TInput, TReturn, TContext> & {};

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
  queryFn: (options: {
    args: TOutput;
    ctx: TContext;
  }) => QueryBuilder<TTable, TSchema, TReturn>,
): QueryDefinition<TTable, TInput, TOutput, TReturn, TContext> & {};

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

  // We wrap the function to add the tag and validator and ensure we do not mutate it in place.
  const f = (options: {args: TOutput; ctx: TContext}) => actualQueryFn(options);
  f.validator = validator;
  f['~'] = 'QueryDefinition' as unknown as QueryDefinitionTypes<
    TTable,
    TInput,
    TOutput,
    TReturn,
    TContext
  >;

  return f;
}

/**
 * Returns a typed version of {@link defineQuery} with the schema and context
 * types pre-specified. This enables better type inference when defining
 * queries.
 *
 * @example
 * ```ts
 * const builder = createBuilder(schema);
 *
 * // With both Schema and Context types
 * const defineAppQuery = defineQueryWithType<AppSchema, AppContext>();
 * const myQuery = defineAppQuery(({ctx}) =>
 *   builder.issue.where('userID', ctx.userID),
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
    queryFn: (options: {
      args: TArgs;
      ctx: TContext;
    }) => QueryBuilder<TTable, TSchema, TReturn>,
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
    queryFn: (options: {
      args: TOutput;
      ctx: TContext;
    }) => QueryBuilder<TTable, TSchema, TReturn>,
  ): QueryDefinition<TTable, TInput, TOutput, TReturn, TContext>;
};
