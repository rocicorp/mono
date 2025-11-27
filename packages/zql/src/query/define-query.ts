// oxlint-disable no-explicit-any
import type {StandardSchemaV1} from '@standard-schema/spec';
import {getValueAtPath} from '../../../shared/src/get-value-at-path.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {asQueryInternals} from './query-internals.ts';
import type {Query} from './query.ts';
import {validateInput} from './validate-input.ts';

const customQueryTag = Symbol();

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
  S extends Schema,
  T extends keyof S['tables'] & string,
  R,
  C,
  Args extends ReadonlyJSONValue | undefined,
  HasArgs extends boolean = false,
> = {
  readonly [customQueryTag]: true;
} & (HasArgs extends true
  ? unknown
  : undefined extends Args
    ? {
        (): CustomQuery<S, T, R, C, Args, true>;
        (args?: Args): CustomQuery<S, T, R, C, Args, true>;
      }
    : {
        (args: Args): CustomQuery<S, T, R, C, Args, true>;
      }) &
  (HasArgs extends true ? {toQuery(ctx: C): Query<S, T, R>} : unknown);

const queryRegistryTag = Symbol();

export function isQueryRegistry<Q extends QueryDefinitions<Schema, any>>(
  obj: unknown,
): obj is QueryRegistry<Q> {
  return (
    typeof obj === 'object' &&
    obj !== null &&
    (obj as any)[queryRegistryTag] === true
  );
}

type SchemaFromQueryDefinitions<QD extends QueryDefinitions<Schema, any>> =
  QD extends QueryDefinitions<infer S, any> ? S : never;

export type QueryRegistry<QD extends QueryDefinitions<Schema, any>> =
  CustomQueriesInner<QD, SchemaFromQueryDefinitions<QD>>;

type CustomQueriesInner<
  QD extends QueryDefinitions<Schema, any>,
  S extends Schema,
> = {
  readonly [K in keyof QD]: QD[K] extends QueryDefinition<
    S,
    infer TTable,
    infer TReturn,
    infer TContext,
    any,
    infer TOutput
  >
    ? CustomQuery<S, TTable, TReturn, TContext, TOutput>
    : QD[K] extends QueryDefinitions<S, any>
      ? CustomQueriesInner<QD[K], S>
      : never;
} & {
  [queryRegistryTag]: true;
};

export type ContextTypeOfQueryRegistry<CQ> =
  CQ extends QueryRegistry<infer QD>
    ? QD extends QueryDefinitions<Schema, infer C>
      ? C
      : never
    : never;

export const defineQueryTag = Symbol();

type QueryDefinitionFunction<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  Args extends ReadonlyJSONValue | undefined,
> = (options: {args: Args; ctx: TContext}) => Query<TSchema, TTable, TReturn>;

/**
 * A query definition is the function callback that you pass into defineQuery.
 */
export type QueryDefinition<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
> = QueryDefinitionFunction<TSchema, TTable, TReturn, TContext, TOutput> & {
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
  return typeof f === 'function' && (f as any)[defineQueryTag];
}

export type QueryDefinitions<S extends Schema, Context> = {
  readonly [key: string]:
    | QueryDefinition<S, any, any, Context, any, any>
    | QueryDefinitions<S, Context>;
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
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TArgs extends ReadonlyJSONValue | undefined,
>(
  queryFn: QueryDefinitionFunction<TSchema, TTable, TReturn, TContext, TArgs>,
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
  queryFn: QueryDefinitionFunction<TSchema, TTable, TReturn, TContext, TOutput>,
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
    | QueryDefinitionFunction<TSchema, TTable, TReturn, TContext, TOutput>,
  queryFn?: QueryDefinitionFunction<
    TSchema,
    TTable,
    TReturn,
    TContext,
    TOutput
  >,
): QueryDefinition<TSchema, TTable, TReturn, TContext, TInput, TOutput> {
  // Handle different parameter patterns
  let validator: StandardSchemaV1<TInput, TOutput> | undefined;
  let actualQueryFn: QueryDefinitionFunction<
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

  // We wrap the function to add the tag and validator and ensure we do not mutate it in place.
  const f = (options: {args: TOutput; ctx: TContext}) => actualQueryFn(options);
  f.validator = validator;
  f[defineQueryTag] = true as const;
  return f;
}

function createCustomQueryBuilder<
  S extends Schema,
  T extends keyof S['tables'] & string,
  R,
  C,
  Args extends ReadonlyJSONValue | undefined,
  HasArgs extends boolean,
>(
  queryDef: QueryDefinition<S, T, R, C, any, Args>,
  name: string,
  args: Args,
  hasArgs: HasArgs,
): CustomQuery<S, T, R, C, Args, HasArgs> {
  const {validator} = queryDef;

  // The callable function that sets args
  const builder = (args: Args) => {
    if (hasArgs) {
      throw new Error('args already set');
    }
    const validatedArgs = validateInput(name, args, validator, 'query');
    return createCustomQueryBuilder<S, T, R, C, Args, true>(
      queryDef,
      name,
      validatedArgs,
      true,
    );
  };

  // Add create method
  builder.toQuery = (ctx: C) => {
    if (!hasArgs) {
      throw new Error('args not set');
    }

    return asQueryInternals(
      queryDef({
        args,
        ctx,
      }),
    ).nameAndArgs(
      name,
      // TODO(arv): Get rid of the array?
      args === undefined ? [] : [args as unknown as ReadonlyJSONValue],
    );
  };

  // Add the tag
  builder[customQueryTag] = true;

  return builder as unknown as CustomQuery<S, T, R, C, Args, HasArgs>;
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
export function defineQueries<QD extends QueryDefinitions<Schema, any>>(
  defs: QD,
): QueryRegistry<QD>;

/**
 * Extends an existing query registry with additional or overriding query
 * definitions. Properties from overrides replace properties from base with
 * the same key.
 *
 * @param base - An existing query registry to extend.
 * @param overrides - New query definitions to add or override.
 * @returns A merged query registry with all queries from both base and overrides.
 */
export function defineQueries<
  QD1 extends QueryDefinitions<Schema, any>,
  QD2 extends QueryDefinitions<Schema, any>,
>(
  base: QueryRegistry<QD1>,
  overrides: QD2,
): QueryRegistry<Omit<QD1, keyof QD2> & QD2>;

/**
 * Merges two query definition objects into a single query registry.
 * Properties from the second parameter replace properties from the first
 * with the same key.
 *
 * @param base - The base query definitions to start with.
 * @param overrides - Additional query definitions to merge in, overriding any
 *   existing definitions with the same key.
 * @returns A merged query registry with all queries from both parameters.
 */
export function defineQueries<
  QD1 extends QueryDefinitions<Schema, any>,
  QD2 extends QueryDefinitions<Schema, any>,
>(base: QD1, overrides: QD2): QueryRegistry<Omit<QD1, keyof QD2> & QD2>;

export function defineQueries<QD extends QueryDefinitions<Schema, any>>(
  defsOrBase: QD | QueryRegistry<QD>,
  overrides?: QueryDefinitions<Schema, unknown>,
): QueryRegistry<any> {
  function processDefinitions(
    definitions: QueryDefinitions<Schema, unknown>,
    path: string[],
  ): Record<string | symbol, any> {
    const result: Record<string | symbol, any> = {
      [queryRegistryTag]: true,
    };

    for (const [key, value] of Object.entries(definitions)) {
      path.push(key);
      const defaultName = path.join('.');

      if (isQueryDefinition(value)) {
        result[key] = createCustomQueryBuilder(
          value,
          defaultName,
          undefined,
          false,
        );
      } else {
        // Nested definitions
        result[key] = processDefinitions(
          value as QueryDefinitions<Schema, unknown>,
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

    return {...base, ...processed} as QueryRegistry<any>;
  }

  return processDefinitions(defsOrBase as QD, []) as QueryRegistry<QD>;
}

/**
 * Returns a typed version of {@link defineQueries} with the schema and context
 * types pre-specified. This enables better type inference when defining
 * queries.
 *
 * @example
 * ```ts
 * const builder = createBuilder(schema);
 *
 * // With both Schema and Context types
 * const defineAppQueries = defineQueriesWithType<AppSchema, AppContext>();
 * const queries = defineAppQueries({
 *   issues: defineQuery(({ctx}) => builder.issue.where('userID', ctx.userID)),
 * });
 *
 * // With just Context type (Schema inferred)
 * const defineAppQueries = defineQueriesWithType<AppContext>();
 * ```
 *
 * @typeParam S - The Zero schema type.
 * @typeParam C - The context type passed to query functions.
 * @returns A function equivalent to {@link defineQueries} but with types
 *   pre-bound.
 */
export function defineQueriesWithType<S extends Schema, C = unknown>(): <
  QD extends QueryDefinitions<S, C>,
>(
  defs: QD,
) => QueryRegistry<QD>;

/**
 * Returns a typed version of {@link defineQueries} with the context type
 * pre-specified.
 *
 * @typeParam C - The context type passed to query functions.
 * @returns A function equivalent to {@link defineQueries} but with the context
 *   type pre-bound.
 */
export function defineQueriesWithType<C>(): <
  QD extends QueryDefinitions<Schema, C>,
>(
  defs: QD,
) => QueryRegistry<QD>;

export function defineQueriesWithType() {
  return defineQueries;
}

export function getQuery<S extends Schema, QD extends QueryDefinitions<S, any>>(
  queries: QueryRegistry<QD>,
  name: string,
):
  | CustomQuery<
      S,
      keyof S['tables'] & string,
      unknown, // return
      unknown, // context
      ReadonlyJSONValue | undefined,
      false
    >
  | undefined {
  return getValueAtPath(queries, name, /[.|]/) as
    | CustomQuery<
        S,
        keyof S['tables'] & string,
        unknown, // return
        unknown, // context
        ReadonlyJSONValue | undefined,
        false
      >
    | undefined;
}

export function mustGetQuery<
  S extends Schema,
  QD extends QueryDefinitions<S, any>,
>(
  queries: QueryRegistry<QD>,
  name: string,
): CustomQuery<
  S,
  keyof S['tables'] & string,
  unknown, // return
  unknown, // context
  ReadonlyJSONValue | undefined,
  false
> {
  const v = getQuery(queries, name);
  if (!v) {
    throw new Error(`Query not found: ${name}`);
  }
  return v;
}
