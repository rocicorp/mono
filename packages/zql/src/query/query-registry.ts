// oxlint-disable no-explicit-any
import type {StandardSchemaV1} from '@standard-schema/spec';
import {deepMerge, type DeepMerge} from '../../../shared/src/deep-merge.ts';
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
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
  THasArgs extends boolean = false,
> = {
  readonly [customQueryTag]: true;
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
  CustomQueriesInner<QD, SchemaFromQueryDefinitions<QD>> & {
    [queryRegistryTag]: true;
  };

type CustomQueriesInner<
  QD extends QueryDefinitions<Schema, any>,
  S extends Schema,
> = {
  readonly [K in keyof QD]: QD[K] extends QueryDefinition<
    infer TTable extends keyof S['tables'] & string,
    infer TInput,
    any,
    S,
    infer TReturn,
    infer TContext
  >
    ? CustomQuery<TTable, TInput, S, TReturn, TContext>
    : QD[K] extends QueryDefinitions<S, any>
      ? CustomQueriesInner<QD[K], S>
      : never;
};

export type ContextTypeOfQueryRegistry<CQ> =
  CQ extends QueryRegistry<infer QD>
    ? QD extends QueryDefinitions<Schema, infer C>
      ? C
      : never
    : never;

export const defineQueryTag = Symbol();

type QueryDefinitionFunction<
  TTable extends keyof TSchema['tables'] & string,
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
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
> = QueryDefinitionFunction<TTable, TOutput, TSchema, TReturn, TContext> & {
  [defineQueryTag]: true;
  validator: StandardSchemaV1<TInput, TOutput> | undefined;
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
  return typeof f === 'function' && (f as any)[defineQueryTag];
}

export type QueryDefinitions<S extends Schema, Context> = {
  readonly [key: string]:
    | QueryDefinition<any, any, any, S, any, Context>
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
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
>(
  queryFn: QueryDefinitionFunction<TTable, TInput, TSchema, TReturn, TContext>,
): QueryDefinition<TTable, TInput, TInput, TSchema, TReturn, TContext>;

// Overload for validator parameter - Input and Output can be different
export function defineQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
>(
  validator: StandardSchemaV1<TInput, TOutput>,
  queryFn: QueryDefinitionFunction<TTable, TOutput, TSchema, TReturn, TContext>,
): QueryDefinition<TTable, TInput, TOutput, TSchema, TReturn, TContext>;

// Implementation
export function defineQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
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
  f[defineQueryTag] = true as const;
  return f;
}

function createCustomQueryBuilder<
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

  // Add the tag
  builder[customQueryTag] = true;

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
  TBase extends QueryDefinitions<Schema, any>,
  TOverrides extends QueryDefinitions<Schema, any>,
>(
  base: QueryRegistry<TBase>,
  overrides: TOverrides,
): QueryRegistry<DeepMerge<TBase, TOverrides>>;

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
  TBase extends QueryDefinitions<Schema, any>,
  TOverrides extends QueryDefinitions<Schema, any>,
>(
  base: TBase,
  overrides: TOverrides,
): QueryRegistry<DeepMerge<TBase, TOverrides>>;

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

    const merged = deepMerge(base, processed) as QueryRegistry<any>;
    merged[queryRegistryTag] = true;
    return merged;
  }

  return processDefinitions(defsOrBase as QD, []) as QueryRegistry<QD>;
}

export function getQuery<S extends Schema, QD extends QueryDefinitions<S, any>>(
  queries: QueryRegistry<QD>,
  name: string,
):
  | CustomQuery<
      keyof S['tables'] & string,
      ReadonlyJSONValue | undefined, // ArgsInput
      S,
      unknown, // return
      unknown, // context
      false
    >
  | undefined {
  return getValueAtPath(queries, name, /[.|]/) as
    | CustomQuery<
        keyof S['tables'] & string,
        ReadonlyJSONValue | undefined, // ArgsInput
        S,
        unknown, // return
        unknown, // context
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
  keyof S['tables'] & string,
  ReadonlyJSONValue | undefined, // ArgsInput
  S,
  unknown, // return
  unknown, // context
  false
> {
  const v = getQuery(queries, name);
  if (!v) {
    throw new Error(`Query not found: ${name}`);
  }
  return v;
}
