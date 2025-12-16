import type {StandardSchemaV1} from '@standard-schema/spec';
import {
  deepMerge,
  isPlainObject,
  type DeepMerge,
} from '../../../shared/src/deep-merge.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import {getValueAtPath} from '../../../shared/src/object-traversal.ts';
import type {
  DefaultContext,
  DefaultSchema,
} from '../../../zero-types/src/default-types.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {asQueryInternals} from './query-internals.ts';
import type {PullRow, Query} from './query.ts';
import {validateInput} from './validate-input.ts';

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
  TSchema extends Schema,
  TReturn,
  TContext,
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

export function isQuery<S extends Schema>(
  value: unknown,
  // oxlint-disable-next-line no-explicit-any
): value is CustomQuery<string, any, any, S, any, any> {
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

// ----------------------------------------------------------------------------
// QueryRegistry types
// ----------------------------------------------------------------------------

export function isQueryRegistry(obj: unknown): obj is AnyQueryRegistry {
  return (
    typeof obj === 'object' &&
    obj !== null &&
    (obj as unknown as {['~']: string})?.['~'] === 'QueryRegistry'
  );
}

export type QueryRegistryTypes<TSchema extends Schema> = 'QueryRegistry' & {
  readonly $schema: TSchema;
};

export type QueryRegistry<
  QD extends QueryDefinitions,
  S extends Schema,
> = ToQueryTree<QD, S> & {
  ['~']: QueryRegistryTypes<S>;
};

export type AnyQueryRegistry = {
  ['~']: QueryRegistryTypes<Schema>;
  [key: string]: unknown;
};

type ToQueryTree<QD extends QueryDefinitions, S extends Schema> = {
  readonly [K in keyof QD]: QD[K] extends AnyQueryDefinition
    ? // pull types from the phantom property
      CustomQuery<
        QD[K]['~']['$tableName'],
        QD[K]['~']['$input'],
        QD[K]['~']['$output'],
        S,
        QD[K]['~']['$return'],
        QD[K]['~']['$context']
      >
    : QD[K] extends QueryDefinitions
      ? ToQueryTree<QD[K], S>
      : never;
};

export type FromQueryTree<QD extends QueryDefinitions, S extends Schema> = {
  readonly [K in keyof QD]: QD[K] extends AnyQueryDefinition
    ? CustomQuery<
        QD[K]['~']['$tableName'],
        // intentionally left as generic to avoid variance issues
        ReadonlyJSONValue | undefined,
        ReadonlyJSONValue | undefined,
        S,
        QD[K]['~']['$return'],
        QD[K]['~']['$context']
      >
    : QD[K] extends QueryDefinitions
      ? FromQueryTree<QD[K], S>
      : never;
}[keyof QD];

export type QueryDefinitions = {
  readonly [key: string]: AnyQueryDefinition | QueryDefinitions;
};

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

export type QueryDefinitionFunction<
  TTable extends string,
  TInput extends ReadonlyJSONValue | undefined,
  TReturn,
  TContext,
> = (options: {args: TInput; ctx: TContext}) => Query<TTable, Schema, TReturn>;

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

  if ('~standard' in validatorOrQueryFn) {
    // defineQuery(validator, queryFn) - with validator
    validator = validatorOrQueryFn;
    actualQueryFn = must(queryFn);
  } else {
    // defineQuery(queryFn) - no validator
    validator = undefined;
    actualQueryFn = validatorOrQueryFn;
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

// ----------------------------------------------------------------------------
// createQuery
// ----------------------------------------------------------------------------

export function createQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TReturn,
  TContext,
>(
  name: string,
  definition: QueryDefinition<TTable, TInput, TOutput, TReturn, TContext>,
): CustomQuery<TTable, TInput, TOutput, TSchema, TReturn, TContext> {
  const {validator} = definition;

  const fn: QueryDefinitionFunction<
    TTable,
    TInput,
    TReturn,
    TContext
  > = options => {
    const validatedArgs = validator
      ? validateInput(name, options.args, validator, 'query')
      : (options.args as unknown as TOutput);

    return asQueryInternals(
      definition.fn({
        args: validatedArgs,
        ctx: options.ctx,
      }),
    ).nameAndArgs(
      name,
      // TODO(arv): Get rid of the array?
      // Send original input args to server (not transformed output)
      options.args === undefined ? [] : [options.args],
    );
  };

  const query = (
    args: TInput,
  ): QueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext> => ({
    args,
    '~': 'QueryRequest' as QueryRequestTypes<
      TTable,
      TInput,
      TOutput,
      TSchema,
      TReturn,
      TContext
    >,
    'query': query as unknown as CustomQuery<
      TTable,
      TInput,
      TOutput,
      TSchema,
      TReturn,
      TContext
    >,
  });

  query.queryName = name;
  query.fn = fn;
  query['~'] = 'Query' as CustomQueryTypes<
    TTable,
    TInput,
    TSchema,
    TReturn,
    TContext
  >;

  return query as unknown as CustomQuery<
    TTable,
    TInput,
    TOutput,
    TSchema,
    TReturn,
    TContext
  >;
}

// ----------------------------------------------------------------------------
// defineQueries
// ----------------------------------------------------------------------------

/**
 * Converts query definitions created with {@link defineQuery} into callable
 * {@link Query} objects that can be invoked with arguments and a context.
 *
 * Query definitions can be nested for organization. The resulting query names
 * are dot-separated paths (e.g., `users.byId`).
 *
 * @example
 * ```ts
 * const builder = createBuilder(schema);
 *
 * const queries = defineQueries({
 *   issues: defineQuery(() => builder.issue.orderBy('created', 'desc')),
 *   users: {
 *     byId: defineQuery(({args}: {args: {id: string}}) =>
 *       builder.user.where('id', args.id),
 *     ),
 *   },
 * });
 *
 * // Usage:
 * const request = queries.issues.byId({id: '123'});
 * const [data] = zero.useQuery(request);
 * ```
 *
 * @param defs - An object containing query definitions or nested objects of
 *   query definitions.
 * @returns An object with the same structure where each query definition is
 *   converted to a {@link CustomQuery}.
 */
export function defineQueries<
  // let QD infer freely so defaults aren't erased by a QueryDefinitions<any, any> constraint
  const QD,
  S extends Schema = DefaultSchema,
>(
  defs: QD & AssertQueryDefinitions<QD>,
): QueryRegistry<EnsureQueryDefinitions<QD>, S>;

export function defineQueries<
  const TBase,
  const TOverrides,
  S extends Schema = DefaultSchema,
>(
  base:
    | QueryRegistry<EnsureQueryDefinitions<TBase>, S>
    | (TBase & AssertQueryDefinitions<TBase>),
  overrides: TOverrides & AssertQueryDefinitions<TOverrides>,
): QueryRegistry<
  DeepMerge<
    EnsureQueryDefinitions<TBase>,
    EnsureQueryDefinitions<TOverrides>,
    AnyQueryDefinition
  >,
  S
>;

export function defineQueries<QD extends QueryDefinitions, S extends Schema>(
  defsOrBase: QD | QueryRegistry<QD, S>,
  overrides?: QueryDefinitions,
): QueryRegistry<QD, S> {
  function processDefinitions(
    definitions: QueryDefinitions,
    path: string[],
  ): Record<string | symbol, unknown> {
    const result: Record<string | symbol, unknown> = {
      ['~']: 'QueryRegistry',
    };

    for (const [key, value] of Object.entries(definitions)) {
      path.push(key);
      const defaultName = path.join('.');

      if (isQueryDefinition(value)) {
        result[key] = createQuery(defaultName, value);
      } else {
        // Nested definitions
        result[key] = processDefinitions(value, path);
      }
      path.pop();
    }

    return result;
  }

  if (overrides !== undefined) {
    // Merge base and overrides

    let base: Record<string | symbol, unknown>;
    if (!isQueryRegistry(defsOrBase)) {
      base = processDefinitions(defsOrBase, []);
    } else {
      base = defsOrBase;
    }

    const processed = processDefinitions(overrides, []);

    const merged = deepMerge(base, processed, isQueryLeaf);
    merged['~'] = 'QueryRegistry';
    return merged as QueryRegistry<QD, S>;
  }

  return processDefinitions(defsOrBase as QD, []) as QueryRegistry<QD, S>;
}

const isQueryLeaf = (value: unknown): boolean =>
  !isPlainObject(value) || isQuery(value);

export type AssertQueryDefinitions<QD> = QD extends QueryDefinitions
  ? unknown
  : never;

export type EnsureQueryDefinitions<QD> = QD extends QueryDefinitions
  ? QD
  : QD extends QueryRegistry<infer InnerQD, infer _S>
    ? InnerQD
    : never;

/**
 * Creates a function that can be used to define queries with a specific schema.
 */
export function defineQueriesWithType<
  TSchema extends Schema,
>(): TypedDefineQueries<TSchema> {
  return defineQueries;
}

/**
 * The return type of defineQueriesWithType. A function matching the
 * defineQueries overloads but with Schema pre-bound.
 */
type TypedDefineQueries<S extends Schema> = {
  // Single definitions
  <QD>(
    definitions: QD & AssertQueryDefinitions<QD>,
  ): QueryRegistry<EnsureQueryDefinitions<QD>, S>;

  // Base and overrides
  <TBase, TOverrides>(
    base:
      | QueryRegistry<EnsureQueryDefinitions<TBase>, S>
      | (TBase & AssertQueryDefinitions<TBase>),
    overrides: TOverrides & AssertQueryDefinitions<TOverrides>,
  ): QueryRegistry<
    DeepMerge<
      EnsureQueryDefinitions<TBase>,
      EnsureQueryDefinitions<TOverrides>,
      AnyQueryDefinition
    >,
    S
  >;
};

// ----------------------------------------------------------------------------
// getQuery / mustGetQuery
// ----------------------------------------------------------------------------

export function getQuery<QD extends QueryDefinitions, S extends Schema>(
  queries: QueryRegistry<QD, S>,
  name: string,
): FromQueryTree<QD, S> | undefined {
  const q = getValueAtPath(queries, name, /[.|]/);
  return q as FromQueryTree<QD, S> | undefined;
}

export function mustGetQuery<QD extends QueryDefinitions, S extends Schema>(
  queries: QueryRegistry<QD, S>,
  name: string,
): FromQueryTree<QD, S> {
  const query = getQuery(queries, name);
  if (query === undefined) {
    throw new Error(`Query not found: ${name}`);
  }
  return query;
}
