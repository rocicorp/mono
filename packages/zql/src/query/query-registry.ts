// oxlint-disable no-explicit-any
import type {StandardSchemaV1} from '@standard-schema/spec';
import {deepMerge, type DeepMerge} from '../../../shared/src/deep-merge.ts';
import type {Expand} from '../../../shared/src/expand.ts';
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

/**
 * CustomQuery is what is returned from defineQueries. It supports a builder
 * pattern where args is set before calling toQuery(context).
 *
 * const queries = defineQueries(...);
 * queries.foo.bar satisfies CustomQuery<...>
 *
 * Usage:
 *   queries.foo(args).toQuery(ctx)
 */
export type CustomQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
  THasArgs extends boolean = false,
> = {
  /**
   * Type-only phantom property to surface query types in a covariant position.
   */
  '~': Expand<
    QueryTypes<TTable, TInput, never, TSchema, TReturn, TContext, THasArgs>
  >;
} & (THasArgs extends true
  ? unknown
  : undefined extends TInput
    ? {
        (): CustomQuery<TTable, TInput, TSchema, TReturn, TContext, true>;
        (
          args?: TInput,
        ): CustomQuery<TTable, TInput, TSchema, TReturn, TContext, true>;
      }
    : {
        (
          args: TInput,
        ): CustomQuery<TTable, TInput, TSchema, TReturn, TContext, true>;
      }) &
  (THasArgs extends true
    ? {toQuery(ctx: TContext): Query<TTable, TSchema, TReturn>}
    : unknown);

export function isQueryRegistry<Q extends QueryDefinitions<any>>(
  obj: unknown,
): obj is QueryRegistry<Q> {
  return (
    typeof obj === 'object' &&
    obj !== null &&
    (obj as any)['~'] === 'QueryRegistry'
  );
}

export type QueryTypes<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput,
  TSchema extends Schema,
  TReturn,
  TContext,
  THasArgs extends boolean,
> = 'Query' & {
  readonly $tableName: TTable;
  readonly $input: TInput;
  readonly $output: TOutput;
  readonly $schema: TSchema;
  readonly $return: TReturn;
  readonly $context: TContext;
  readonly $hasArgs: THasArgs;
};

export type QueryDefinitionTypes<
  TTable extends string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput,
  TReturn,
  TSchema extends Schema,
  TContext,
> = 'QueryDefinition' & {
  readonly $tableName: TTable;
  readonly $input: TInput;
  readonly $output: TOutput;
  readonly $return: TReturn;
  readonly $schema: TSchema;
  readonly $context: TContext;
};

export type QueryRegistryTypes = 'QueryRegistry';

export type QueryRegistry<QD extends QueryDefinitions<any>> =
  ToQueryTree<QD> & {
    ['~']: Expand<QueryRegistryTypes>;
  };

type AnyQueryDefinition = QueryDefinition<any, any, any, any, any, any>;

type ToQueryTree<QD extends QueryDefinitions<any>> = {
  readonly [K in keyof QD]: QD[K] extends AnyQueryDefinition
    ? // pull types from the phantom property
      CustomQuery<
        QD[K]['~']['$tableName'],
        QD[K]['~']['$input'],
        QD[K]['~']['$schema'],
        QD[K]['~']['$return'],
        QD[K]['~']['$context'],
        false
      >
    : QD[K] extends QueryDefinitions<any>
      ? ToQueryTree<QD[K]>
      : never;
};

export type FromQueryTree<QD extends QueryDefinitions<any>> = {
  readonly [K in keyof QD]: QD[K] extends AnyQueryDefinition
    ? CustomQuery<
        QD[K]['~']['$tableName'],
        ReadonlyJSONValue | undefined, // intentionally left as generic to avoid variance issues
        QD[K]['~']['$schema'],
        QD[K]['~']['$return'],
        QD[K]['~']['$context'],
        false
      >
    : QD[K] extends QueryDefinitions<any>
      ? FromQueryTree<QD[K]>
      : never;
}[keyof QD];

type QueryDefinitionFunction<
  TTable extends string,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TReturn,
  TContext,
> = (options: {
  args: TOutput;
  ctx: TContext;
}) => Query<TTable, TSchema, TReturn>;

/**
 * A query definition is the return type of `defineQuery()`.
 */
export type QueryDefinition<
  TTable extends string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
> = QueryDefinitionFunction<TTable, TOutput, TSchema, TReturn, TContext> & {
  'validator': StandardSchemaV1<TInput, TOutput> | undefined;

  /**
   * Type-only phantom property to surface query types in a covariant position.
   */
  readonly '~': Expand<
    QueryDefinitionTypes<TTable, TInput, TOutput, TReturn, TSchema, TContext>
  >;
};

export function isQueryDefinition<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
>(
  f: unknown,
): f is QueryDefinition<TTable, TInput, TOutput, TSchema, TReturn, TContext> {
  return typeof f === 'function' && (f as any)['~'] === 'QueryDefinition';
}

export type QueryDefinitions<Context> = {
  readonly [key: string]:
    | QueryDefinition<any, any, any, any, any, Context>
    | QueryDefinitions<Context>;
};

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
  }) => Query<TTable, TSchema, TReturn>,
): QueryDefinition<TTable, TInput, TInput, TSchema, TReturn, TContext> & {};

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
  }) => Query<TTable, TSchema, TReturn>,
): QueryDefinition<TTable, TInput, TOutput, TSchema, TReturn, TContext> & {};

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
    | QueryDefinitionFunction<TTable, TOutput, TSchema, TReturn, TContext>,
  queryFn?: QueryDefinitionFunction<
    TTable,
    TOutput,
    TSchema,
    TReturn,
    TContext
  >,
): QueryDefinition<TTable, TInput, TOutput, TSchema, TReturn, TContext> {
  // Handle different parameter patterns
  let validator: StandardSchemaV1<TInput, TOutput> | undefined;
  let actualQueryFn: QueryDefinitionFunction<
    TTable,
    TOutput,
    TSchema,
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
    TSchema,
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
    }) => Query<TTable, TSchema, TReturn>,
  ): QueryDefinition<TTable, TArgs, TArgs, TSchema, TReturn, TContext>;

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
    }) => Query<TTable, TSchema, TReturn>,
  ): QueryDefinition<TTable, TInput, TOutput, TSchema, TReturn, TContext>;
};

export function createCustomQueryBuilder<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TReturn,
  TContext,
  THasArgs extends boolean,
>(
  queryDef: QueryDefinition<
    TTable,
    TInput,
    TOutput,
    TSchema,
    TReturn,
    TContext
  >,
  name: string,
  inputArgs: TInput,
  validatedArgs: TOutput,
  hasArgs: THasArgs,
): CustomQuery<TTable, TInput, TSchema, TReturn, TContext, THasArgs> {
  const {validator} = queryDef;

  // The callable function that sets args
  const builder = (args: TInput) => {
    if (hasArgs) {
      throw new Error('args already set');
    }
    const validated = validateInput(name, args, validator, 'query');
    return createCustomQueryBuilder<
      TTable,
      TInput,
      TOutput,
      TSchema,
      TReturn,
      TContext,
      true
    >(queryDef, name, args, validated, true);
  };

  // Add create method
  builder.toQuery = (ctx: TContext) => {
    if (!hasArgs) {
      throw new Error('args not set');
    }

    return asQueryInternals(
      queryDef({
        args: validatedArgs,
        ctx,
      }),
    ).nameAndArgs(
      name,
      // TODO(arv): Get rid of the array?
      // Send original input args to server (not transformed output)
      inputArgs === undefined
        ? []
        : [inputArgs as unknown as ReadonlyJSONValue],
    );
  };

  // Add the phantom property
  builder['~'] = 'CustomQuery';

  return builder as unknown as CustomQuery<
    TTable,
    TInput,
    TSchema,
    TReturn,
    TContext,
    THasArgs
  >;
}

/**
 * Converts query definitions created with {@link defineQuery} into callable
 * {@link CustomQuery} objects that can be invoked with arguments and a context.
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
 * const q = queries.issues().toQuery(ctx);
 * const q2 = queries.users.byId({id: '123'}).toQuery(ctx);
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
>(
  defs: QD & AssertQueryDefinitions<QD>,
): QueryRegistry<EnsureQueryDefinitions<QD>>;

export function defineQueries<TBase, TOverrides>(
  base:
    | QueryRegistry<EnsureQueryDefinitions<TBase>>
    | (TBase & AssertQueryDefinitions<TBase>),
  overrides: TOverrides & AssertQueryDefinitions<TOverrides>,
): QueryRegistry<
  DeepMerge<EnsureQueryDefinitions<TBase>, EnsureQueryDefinitions<TOverrides>>
>;

export function defineQueries<QD extends QueryDefinitions<any>>(
  defsOrBase: QD | QueryRegistry<QD>,
  overrides?: QueryDefinitions<any>,
): QueryRegistry<any> {
  function processDefinitions(
    definitions: QueryDefinitions<unknown>,
    path: string[],
  ): Record<string | symbol, any> {
    const result: Record<string | symbol, any> = {
      ['~']: 'QueryRegistry',
    };

    for (const [key, value] of Object.entries(definitions)) {
      path.push(key);
      const defaultName = path.join('.');

      if (isQueryDefinition(value)) {
        result[key] = createCustomQueryBuilder(
          value,
          defaultName,
          undefined,
          undefined,
          false,
        );
      } else {
        // Nested definitions
        result[key] = processDefinitions(
          value as QueryDefinitions<unknown>,
          path,
        );
      }
      path.pop();
    }

    return result;
  }

  if (overrides !== undefined) {
    // Merge base and overrides

    let base: Record<string | symbol, any>;
    if (!isQueryRegistry(defsOrBase)) {
      base = processDefinitions(defsOrBase as QD, []);
    } else {
      base = defsOrBase;
    }

    const processed = processDefinitions(overrides, []);

    const merged = deepMerge(base, processed);
    merged['~'] = 'QueryRegistry';
    return merged as QueryRegistry<any>;
  }

  return processDefinitions(defsOrBase as QD, []) as QueryRegistry<QD>;
}

type AssertQueryDefinitions<QD> =
  QD extends QueryDefinitions<any> ? unknown : never;

type EnsureQueryDefinitions<QD> = QD extends QueryDefinitions<any> ? QD : never;

export function getQuery<QD extends QueryDefinitions<any>>(
  queries: QueryRegistry<QD>,
  name: string,
): FromQueryTree<QD> | undefined {
  const q = getValueAtPath(queries, name, /[.|]/);
  return q as FromQueryTree<QD> | undefined;
}

export function mustGetQuery<QD extends QueryDefinitions<any>>(
  queries: QueryRegistry<QD>,
  name: string,
): FromQueryTree<QD> {
  const query = getQuery(queries, name);
  if (query === undefined) {
    throw new Error(`Query not found: ${name}`);
  }
  return query;
}
