import {
  deepMerge,
  isPlainObject,
  type DeepMerge,
} from '../../../shared/src/deep-merge.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {getValueAtPath} from '../../../shared/src/object-traversal.ts';
import type {DefaultSchema} from '../../../zero-types/src/default-types.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {
  isQuery,
  isQueryDefinition,
  type AnyQueryDefinition,
  type CustomQuery,
  type CustomQueryTypes,
  type QueryDefinition,
  type QueryDefinitionFunction,
  type QueryRequest,
  type QueryRequestTypes,
} from './define-query.ts';
import {asQueryInternals} from './query-internals.ts';
import {validateInput} from './validate-input.ts';

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
      base = processDefinitions(defsOrBase as QD, []);
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

export type AssertQueryDefinitions<QD> = QD extends QueryDefinitions
  ? unknown
  : never;

export type EnsureQueryDefinitions<QD> = QD extends QueryDefinitions
  ? QD
  : QD extends QueryRegistry<infer InnerQD, infer _S>
    ? InnerQD
    : never;

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
