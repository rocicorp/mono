/* eslint-disable @typescript-eslint/no-explicit-any */
import type {Expand, ExpandRecursive} from '../../../shared/src/expand.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {type AST, type SimpleOperator} from '../../../zero-protocol/src/ast.ts';
import type {Schema as ZeroSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {
  LastInTuple,
  SchemaValueToTSType,
  SchemaValueWithCustomType,
  TableSchema,
} from '../../../zero-schema/src/table-schema.ts';
import type {Format, ViewFactory} from '../ivm/view.ts';
import type {ExpressionFactory, ParameterReference} from './expression.ts';
import type {CustomQueryID} from './named.ts';
import type {QueryDelegate} from './query-delegate.ts';
import type {TTL} from './ttl.ts';
import type {TypedView} from './typed-view.ts';

type Selector<E extends TableSchema> = keyof E['columns'];
export type NoCompoundTypeSelector<T extends TableSchema> = Exclude<
  Selector<T>,
  JsonSelectors<T> | ArraySelectors<T>
>;

type JsonSelectors<E extends TableSchema> = {
  [K in keyof E['columns']]: E['columns'][K] extends {type: 'json'} ? K : never;
}[keyof E['columns']];

type ArraySelectors<E extends TableSchema> = {
  [K in keyof E['columns']]: E['columns'][K] extends SchemaValueWithCustomType<
    any[]
  >
    ? K
    : never;
}[keyof E['columns']];

export type GetFilterType<
  TSchema extends TableSchema,
  TColumn extends keyof TSchema['columns'],
  TOperator extends SimpleOperator,
> = TOperator extends 'IS' | 'IS NOT'
  ? // SchemaValueToTSType adds null if the type is optional, but we add null
    // no matter what for dx reasons. See:
    // https://github.com/rocicorp/mono/pull/3576#discussion_r1925792608
    SchemaValueToTSType<TSchema['columns'][TColumn]> | null
  : TOperator extends 'IN' | 'NOT IN'
    ? // We don't want to compare to null in where clauses because it causes
      // confusing results:
      // https://zero.rocicorp.dev/docs/reading-data#comparing-to-null
      readonly Exclude<SchemaValueToTSType<TSchema['columns'][TColumn]>, null>[]
    : Exclude<SchemaValueToTSType<TSchema['columns'][TColumn]>, null>;

export type AvailableRelationships<
  TTable extends string,
  TSchema extends ZeroSchema,
> = keyof TSchema['relationships'][TTable] & string;

export type DestTableName<
  TTable extends string,
  TSchema extends ZeroSchema,
  TRelationship extends string,
> = LastInTuple<TSchema['relationships'][TTable][TRelationship]>['destSchema'];

type DestRow<
  TTable extends string,
  TSchema extends ZeroSchema,
  TRelationship extends string,
> = TSchema['relationships'][TTable][TRelationship][0]['cardinality'] extends 'many'
  ? PullRow<DestTableName<TTable, TSchema, TRelationship>, TSchema>
  : PullRow<DestTableName<TTable, TSchema, TRelationship>, TSchema> | undefined;

type AddSubreturn<TExistingReturn, TSubselectReturn, TAs extends string> = {
  readonly [K in TAs]: undefined extends TSubselectReturn
    ? TSubselectReturn
    : readonly TSubselectReturn[];
} extends infer TNewRelationship
  ? undefined extends TExistingReturn
    ? (Exclude<TExistingReturn, undefined> & TNewRelationship) | undefined
    : TExistingReturn & TNewRelationship
  : never;

export type PullTableSchema<
  TTable extends string,
  TSchemas extends ZeroSchema,
> = TSchemas['tables'][TTable];

export type PullRow<TTable extends string, TSchema extends ZeroSchema> = {
  readonly [K in keyof PullTableSchema<
    TTable,
    TSchema
  >['columns']]: SchemaValueToTSType<
    PullTableSchema<TTable, TSchema>['columns'][K]
  >;
};

export type Row<T extends TableSchema | Query<ZeroSchema, string, any>> =
  T extends TableSchema
    ? {
        readonly [K in keyof T['columns']]: SchemaValueToTSType<
          T['columns'][K]
        >;
      }
    : T extends Query<ZeroSchema, string, infer TReturn>
      ? TReturn
      : never;

/**
 * A hybrid query that runs on both client and server.
 * Results are returned immediately from the client followed by authoritative
 * results from the server.
 *
 * Queries are transactional in that all queries update at once when a new transaction
 * has been committed on the client or server. No query results will reflect stale state.
 *
 * A query can be:
 * - {@linkcode materialize | materialize}
 * - awaited (`then`/{@linkcode run})
 * - {@linkcode preload | preloaded}
 *
 * The normal way to use a query would be through your UI framework's bindings (e.g., useQuery(q))
 * or within a custom mutator.
 *
 * `materialize` and `run/then` are provided for more advanced use cases.
 * Remember that any `view` returned by `materialize` must be destroyed.
 *
 * A query can be run as a 1-shot query by awaiting it. E.g.,
 *
 * ```ts
 * const result = await z.query.issue.limit(10);
 * ```
 *
 * For more information on how to use queries, see the documentation:
 * https://zero.rocicorp.dev/docs/reading-data
 *
 * @typeParam TSchema The database schema type extending ZeroSchema
 * @typeParam TTable The name of the table being queried, must be a key of TSchema['tables']
 * @typeParam TReturn The return type of the query, defaults to PullRow<TTable, TSchema>
 */
export interface Query<
  TSchema extends ZeroSchema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> extends PromiseLike<HumanReadable<TReturn>> {
  /**
   * Format is used to specify the shape of the query results. This is used by
   * {@linkcode one} and it also describes the shape when using
   * {@linkcode related}.
   */
  readonly format: Format;

  /**
   * A string that uniquely identifies this query. This can be used to determine
   * if two queries are the same.
   *
   * The hash of a custom query, on the client, is the hash of its AST.
   * The hash of a custom query, on the server, is the hash of its name and args.
   *
   * The first allows many client-side queries to be pinned to the same backend query.
   * The second ensures we do not invoke a named query on the backend more than once for the same `name:arg` pairing.
   *
   * If the query.hash was of `name:args` then `useQuery` would de-dupe
   * queries with divergent ASTs.
   *
   * QueryManager will hash based on `name:args` since it is speaking with
   * the server which tracks queries by `name:args`.
   */
  hash(): string;
  readonly ast: AST;
  readonly customQueryID: CustomQueryID | undefined;

  nameAndArgs(
    name: string,
    args: ReadonlyArray<ReadonlyJSONValue>,
  ): Query<TSchema, TTable, TReturn>;
  delegate(delegate: QueryDelegate): Query<TSchema, TTable, TReturn>;

  /**
   * Related is used to add a related query to the current query. This is used
   * for subqueries and joins. These relationships are defined in the
   * relationships section of the schema. The result of the query will
   * include the related rows in the result set as a sub object of the row.
   *
   * ```typescript
   * const row = await z.query.users
   *   .related('posts');
   * // {
   * //   id: '1',
   * //   posts: [
   * //     ...
   * //   ]
   * // }
   * ```
   * If you want to add a subquery to the related query, you can do so by
   * providing a callback function that receives the related query as an argument.
   *
   * ```typescript
   * const row = await z.query.users
   *   .related('posts', q => q.where('published', true));
   * // {
   * //   id: '1',
   * //   posts: [
   * //     {published: true, ...},
   * //     ...
   * //   ]
   * // }
   * ```
   *
   * @param relationship The name of the relationship
   */
  related<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
  ): Query<
    TSchema,
    TTable,
    AddSubreturn<
      TReturn,
      DestRow<TTable, TSchema, TRelationship>,
      TRelationship
    >
  >;
  related<
    TRelationship extends AvailableRelationships<TTable, TSchema>,
    TSub extends Query<TSchema, string, any>,
  >(
    relationship: TRelationship,
    cb: (
      q: Query<
        TSchema,
        DestTableName<TTable, TSchema, TRelationship>,
        DestRow<TTable, TSchema, TRelationship>
      >,
    ) => TSub,
  ): Query<
    TSchema,
    TTable,
    AddSubreturn<
      TReturn,
      TSub extends Query<TSchema, string, infer TSubReturn>
        ? TSubReturn
        : never,
      TRelationship
    >
  >;

  /**
   * Represents a condition to filter the query results.
   *
   * @param field The column name to filter on.
   * @param op The operator to use for filtering.
   * @param value The value to compare against.
   *
   * @returns A new query instance with the applied filter.
   *
   * @example
   *
   * ```typescript
   * const query = db.query('users')
   *   .where('age', '>', 18)
   *   .where('name', 'LIKE', '%John%');
   * ```
   */
  where<
    TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>,
    TOperator extends SimpleOperator,
  >(
    field: TSelector,
    op: TOperator,
    value:
      | GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, TOperator>
      | ParameterReference,
  ): Query<TSchema, TTable, TReturn>;
  /**
   * Represents a condition to filter the query results.
   *
   * This overload is used when the operator is '='.
   *
   * @param field The column name to filter on.
   * @param value The value to compare against.
   *
   * @returns A new query instance with the applied filter.
   *
   * @example
   * ```typescript
   * const query = db.query('users')
   *  .where('age', 18)
   * ```
   */
  where<
    TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>,
  >(
    field: TSelector,
    value:
      | GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, '='>
      | ParameterReference,
  ): Query<TSchema, TTable, TReturn>;

  /**
   * Represents a condition to filter the query results.
   *
   * @param expressionFactory A function that takes a query builder and returns an expression.
   *
   * @returns A new query instance with the applied filter.
   *
   * @example
   * ```typescript
   * const query = db.query('users')
   *   .where(({cmp, or}) => or(cmp('age', '>', 18), cmp('name', 'LIKE', '%John%')));
   * ```
   */
  where(
    expressionFactory: ExpressionFactory<TSchema, TTable>,
  ): Query<TSchema, TTable, TReturn>;

  whereExists(
    relationship: AvailableRelationships<TTable, TSchema>,
  ): Query<TSchema, TTable, TReturn>;
  whereExists<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
    cb: (
      q: Query<TSchema, DestTableName<TTable, TSchema, TRelationship>>,
    ) => Query<TSchema, string>,
  ): Query<TSchema, TTable, TReturn>;

  /**
   * Skips the rows of the query until row matches the given row. If opts is
   * provided, it determines whether the match is inclusive.
   *
   * @param row The row to start from. This is a partial row object and only the provided
   *            fields will be used for the comparison.
   * @param opts Optional options object that specifies whether the match is inclusive.
   *             If `inclusive` is true, the row will be included in the result.
   *             If `inclusive` is false, the row will be excluded from the result and the result
   *             will start from the next row.
   *
   * @returns A new query instance with the applied start condition.
   */
  start(
    row: Partial<PullRow<TTable, TSchema>>,
    opts?: {inclusive: boolean} | undefined,
  ): Query<TSchema, TTable, TReturn>;

  /**
   * Limits the number of rows returned by the query.
   * @param limit The maximum number of rows to return.
   *
   * @returns A new query instance with the applied limit.
   */
  limit(limit: number): Query<TSchema, TTable, TReturn>;

  /**
   * Orders the results by a specified column. If multiple orderings are
   * specified, the results will be ordered by the first column, then the
   * second column, and so on.
   *
   * @param field The column name to order by.
   * @param direction The direction to order the results (ascending or descending).
   *
   * @returns A new query instance with the applied order.
   */
  orderBy<TSelector extends Selector<PullTableSchema<TTable, TSchema>>>(
    field: TSelector,
    direction: 'asc' | 'desc',
  ): Query<TSchema, TTable, TReturn>;

  /**
   * Limits the number of rows returned by the query to a single row and then
   * unpacks the result so that you do not get an array of rows but a single
   * row. This is useful when you expect only one row to be returned and want to
   * work with the row directly.
   *
   * If the query returns no rows, the result will be `undefined`.
   *
   * @returns A new query instance with the applied limit to one row.
   */
  one(): Query<TSchema, TTable, TReturn | undefined>;

  /**
   * Creates a materialized view of the query. This is a view that will be kept
   * in memory and updated as the query results change.
   *
   * Most of the time you will want to use the `useQuery` hook or the
   * `run`/`then` method to get the results of a query. This method is only
   * needed if you want to access to lower level APIs of the view.
   *
   * @param ttl Time To Live. This is the amount of time to keep the rows
   *            associated with this query after `TypedView.destroy`
   *            has been called.
   */
  materialize(ttl?: TTL): TypedView<HumanReadable<TReturn>>;
  /**
   * Creates a custom materialized view using a provided factory function. This
   * allows framework-specific bindings (like SolidJS, Vue, etc.) to create
   * optimized views.
   *
   * @param factory A function that creates a custom view implementation
   * @param ttl Optional Time To Live for the view's data after destruction
   * @returns A custom view instance of type {@linkcode T}
   *
   * @example
   * ```ts
   * const view = query.materialize(createSolidView, '1m');
   * ```
   */
  materialize<T>(
    factory: ViewFactory<TSchema, TTable, TReturn, T>,
    ttl?: TTL,
  ): T;

  /**
   * Executes the query and returns the result once. The `options` parameter
   * specifies whether to wait for complete results or return immediately.
   *
   * - `{type: 'unknown'}`: Returns a snapshot of the data immediately.
   * - `{type: 'complete'}`: Waits for the latest, complete results from the server.
   *
   * By default, `run` uses `{type: 'unknown'}` to avoid waiting for the server.
   *
   * `Query` implements `PromiseLike`, and calling `then` on it will invoke `run`
   * with the default behavior (`unknown`).
   *
   * @param options Options to control the result type. Defaults to `{type: 'unknown'}`.
   * @returns A promise resolving to the query result.
   *
   * @example
   * ```js
   * const result = await query.run({type: 'complete'});
   * ```
   */
  run(options?: RunOptions): Promise<HumanReadable<TReturn>>;

  /**
   * Preload loads the data into the clients cache without keeping it in memory.
   * This is useful for preloading data that will be used later.
   *
   * @param options Options for preloading the query.
   * @param options.ttl Time To Live. This is the amount of time to keep the rows
   *                  associated with this query after {@linkcode cleanup} has
   *                  been called.
   */
  preload(options?: PreloadOptions): {
    cleanup: () => void;
    complete: Promise<void>;
  };
}

export type PreloadOptions = {
  /**
   * Time To Live. This is the amount of time to keep the rows associated with
   * this query after {@linkcode cleanup} has been called.
   */
  ttl?: TTL | undefined;
};

/**
 * A helper type that tries to make the type more readable.
 */
export type HumanReadable<T> = undefined extends T ? Expand<T> : Expand<T>[];

/**
 * A helper type that tries to make the type more readable.
 */
// Note: opaque types expand incorrectly.
export type HumanReadableRecursive<T> = undefined extends T
  ? ExpandRecursive<T>
  : ExpandRecursive<T>[];

/**
 * The kind of results we want to wait for when using {@linkcode run} on {@linkcode Query}.
 *
 * `unknown` means we don't want to wait for the server to return results. The result is a
 * snapshot of the data at the time the query was run.
 *
 * `complete` means we want to ensure that we have the latest result from the server. The
 * result is a complete and up-to-date view of the data. In some cases this means that we
 * have to wait for the server to return results. To ensure that we have the result for
 * this query you can preload it before calling run. See {@link preload}.
 *
 * By default, `run` uses `{type: 'unknown'}` to avoid waiting for the server.
 */
export type RunOptions = {
  type: 'unknown' | 'complete';
};

export const DEFAULT_RUN_OPTIONS_UNKNOWN = {
  type: 'unknown',
} as const;

export const DEFAULT_RUN_OPTIONS_COMPLETE = {
  type: 'complete',
} as const;
