/* oxlint-disable @typescript-eslint/no-explicit-any */
import type {Expand, ExpandRecursive} from '../../../shared/src/expand.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {type SimpleOperator} from '../../../zero-protocol/src/ast.ts';
import type {DefaultSchema} from '../../../zero-types/src/default-types.ts';
import type {
  SchemaValueToTSType,
  SchemaValueWithCustomType,
} from '../../../zero-types/src/schema-value.ts';
import type {
  LastInTuple,
  TableSchema,
  Schema as ZeroSchema,
} from '../../../zero-types/src/schema.ts';
import type {ViewFactory} from '../ivm/view.ts';
import type {ExpressionFactory, ParameterReference} from './expression.ts';
import type {TTL} from './ttl.ts';
import type {TypedView} from './typed-view.ts';

type Selector<E extends TableSchema> = keyof E['columns'];

export type NoCompoundTypeSelector<T extends TableSchema> = Exclude<
  Selector<T>,
  JsonSelectors<T> | ArraySelectors<T>
>;

export type JsonSelectors<E extends TableSchema> = {
  [K in keyof E['columns']]: E['columns'][K] extends {type: 'json'} ? K : never;
}[keyof E['columns']];

type ArraySelectors<E extends TableSchema> = {
  [K in keyof E['columns']]: E['columns'][K] extends SchemaValueWithCustomType<
    any[]
  >
    ? K
    : never;
}[keyof E['columns']];

export type QueryReturn<Q> = Q extends Query<any, any, infer R> ? R : never;

export type QueryTable<Q> = Q extends Query<infer T, any, any> ? T : never;

export type ExistsOptions = {
  /**
   * Controls the join direction.
   * - `true`: Force the join to be flipped (child drives the join).
   * - `false`: Force the join to stay as a semi-join (parent drives).
   * - `undefined`: Let the planner decide the optimal direction.
   */
  flip?: boolean;

  /**
   * @experimental
   *
   * When true and the subquery is known to return at most one row, the
   * server pre-resolves the matching value from the related table and
   * rewrites the condition as a direct field comparison at hydration time.
   * This avoids a join at query time, and provides more information
   * to the query planner, often resulting in more efficient query plans.
   *
   * However, this can be less efficient if the subquery's result changes
   * frequently, as the query will need to be rehydrated whenever the
   * subquery's result changes.
   *
   * Only supported for one-hop relationships. Throws if used with
   * junction (two-hop) relationships.
   */
  scalar?: boolean;
};

export type GetFilterType<
  TSchema extends TableSchema,
  TColumn extends keyof TSchema['columns'],
  TOperator extends SimpleOperator,
> = GetFilterTypeFromTSType<
  SchemaValueToTSType<TSchema['columns'][TColumn]>,
  TOperator
>;

/**
 * The allowed comparison-value type for a left operand whose TypeScript type is
 * `TS`, given the operator. Used both by {@link GetFilterType} (column operands)
 * and by `cmp` for {@link ExpressionBuilder.json} operands (JSON-path leaves).
 */
export type GetFilterTypeFromTSType<
  TS,
  TOperator extends SimpleOperator,
> = TOperator extends 'IS' | 'IS NOT'
  ?
      // SchemaValueToTSType adds null if the type is optional, but we add null
      // no matter what for dx reasons. See:
      // https://github.com/rocicorp/mono/pull/3576#discussion_r1925792608
      TS | null | undefined
  : TOperator extends 'IN' | 'NOT IN'
    ? // We don't want to compare to null in where clauses because it causes
      // confusing results:
      // https://zero.rocicorp.dev/docs/reading-data#comparing-to-null
      readonly Exclude<TS, null>[]
    : Exclude<TS, null> | undefined;

/**
 * Resolves a single JSON path segment `K` against value type `T`: an object key
 * (`NonNullable<T>[K]`) or an array index (the element type). An unresolvable
 * step — e.g. into an untyped `json()` whose type is `ReadonlyJSONValue` —
 * yields `ReadonlyJSONValue`, keeping untyped paths usable rather than erroring.
 *
 * `NonNullable<T>` is applied because the runtime collapses a null/undefined
 * intermediate to `null`; the leaf is shaped off the non-null value, and
 * operator-level null handling lives in {@link GetFilterTypeFromTSType}.
 */
type JsonStep<T, K> = K extends keyof NonNullable<T>
  ? NonNullable<T>[K]
  : NonNullable<T> extends readonly (infer E)[]
    ? E
    : ReadonlyJSONValue;

/**
 * Maximum JSON path depth that {@link ValueAtPath}/{@link ValidJsonPath} type.
 * Paths deeper than this keep working at runtime but lose static leaf-typing /
 * segment-validation past this depth.
 *
 * This is essentially free: for a fixed (`const`) tuple path the recursion stops
 * when the path is exhausted, not at the budget, so the budget only bounds the
 * rare non-tuple `(string | number)[]` case. The hard ceiling is ~999 (above
 * that, building the `Tuple` budget hits TypeScript's 1000-deep tail-recursion
 * limit — TS2589); 32 is far beyond any real JSON path, with ~30× margin.
 */
type MaxJsonPathDepth = 32;

/**
 * A tuple of length `N`, used as a decrementing recursion budget (each step drops
 * one element; the empty tuple is the base case). A counter avoids a hand-written
 * decrement table and lets {@link MaxJsonPathDepth} be a single number.
 */
type Tuple<N extends number, A extends unknown[] = []> = A['length'] extends N
  ? A
  : Tuple<N, [unknown, ...A]>;

type JsonDepthBudget = Tuple<MaxJsonPathDepth>;

/**
 * The type at JSON path `P` inside value type `T`, segments applied
 * left-to-right. Paths deeper than {@link MaxJsonPathDepth} fall back to
 * `ReadonlyJSONValue`.
 *
 * Recursion is bounded by the `D` budget tuple. **`T` must be a concrete type, not
 * a deferred schema lookup** — `cmp` walks the column type captured (concretely) by
 * `json` at its call site, *not* `SchemaValueToTSType<...[TColumn]>` against a
 * generic `TSchema`. Walking a deferred schema type in `cmp`'s signature defeats
 * TypeScript's structural comparison of `ExpressionBuilder` and cascades into
 * widespread `Query`/`ExpressionBuilder` assignability failures.
 */
export type ValueAtPath<
  T,
  P extends readonly (string | number)[],
  D extends unknown[] = JsonDepthBudget,
> = D extends [unknown, ...infer DR]
  ? P extends readonly [
      infer H,
      ...infer R extends readonly (string | number)[],
    ]
    ? ValueAtPath<JsonStep<T, H>, R, DR>
    : T
  : ReadonlyJSONValue;

/**
 * The valid next path segments at value type `T`: object keys, an array index
 * (`number`), or `string | number` for an untyped (`ReadonlyJSONValue`) value.
 * A scalar leaf has no further segments (`never`). Used by {@link ValidJsonPath}
 * to constrain/autocomplete `json()` path arguments (Tier 2).
 */
type JsonKeysOf<T> =
  NonNullable<ReadonlyJSONValue> extends NonNullable<T>
    ? // untyped `json()` (`ReadonlyJSONValue`) / `any` — allow any segment
        string | number
    : NonNullable<T> extends readonly unknown[]
      ? number
      : NonNullable<T> extends object
        ? Extract<keyof NonNullable<T>, string | number>
        : never;

/**
 * Validates/autocompletes a `json()` path tuple `P` against value type `T`: maps
 * `P` to a tuple whose element at each position is the set of keys valid *there*
 * (computed by stepping through the preceding segments). Used as `...path: P &
 * ValidJsonPath<T, P>`, so an invalid segment fails to satisfy the allowed-keys
 * union at its position — and editors offer the allowed keys as completions.
 *
 * Recursion is bounded by the `D` counter; segments past depth `D` are left
 * unconstrained. As with {@link ValueAtPath}, `T` must be concrete (see that
 * type's note) — `json` resolves the column type at its call site before this runs.
 */
export type ValidJsonPath<
  T,
  P extends readonly (string | number)[],
  D extends unknown[] = JsonDepthBudget,
> = D extends [unknown, ...infer DR]
  ? P extends readonly [
      infer H,
      ...infer R extends readonly (string | number)[],
    ]
    ? readonly [JsonKeysOf<T>, ...ValidJsonPath<JsonStep<T, H>, R, DR>]
    : P
  : P; // past supported depth — leave the rest unconstrained

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
  TTable extends keyof ZeroSchema['tables'] & string,
  TSchemas extends ZeroSchema,
> = TSchemas['tables'][TTable];

export type PullRow<
  TTable extends keyof ZeroSchema['tables'] & string,
  TSchema extends ZeroSchema = DefaultSchema,
> = {
  readonly [K in keyof PullTableSchema<
    TTable,
    TSchema
  >['columns']]: SchemaValueToTSType<
    PullTableSchema<TTable, TSchema>['columns'][K]
  >;
} & {};

type RowNamespace<S extends ZeroSchema | TypeError> = S extends ZeroSchema
  ? {
      readonly [K in keyof S['tables'] &
        string]: S['tables'][K] extends TableSchema
        ? Row<S['tables'][K]>
        : {
            error: `The table schema for table ${K & string} you have registered with \`declare module '@rocicorp/zero'\` is incorrect.`;
            registeredTableSchema: S['tables'][K];
          };
    }
  : S;

export type Row<
  T extends ZeroSchema | TableSchema | AnyQueryLike = DefaultSchema,
> = T extends ZeroSchema
  ? RowNamespace<T>
  : T extends TableSchema
    ? {
        readonly [K in keyof T['columns']]: SchemaValueToTSType<
          T['columns'][K]
        >;
      }
    : T extends AnyQueryLike
      ? QueryRowType<T>
      : never;

/**
 * The shape of a CustomQuery's phantom type property.
 * CustomQuery has '~' containing CustomQueryTypes which extends 'Query' & {...}
 */
type CustomQueryPhantom = {
  readonly '~': {
    readonly $return: unknown;
  };
};

/**
 * A Query, CustomQuery, or a function returning either.
 */
export type AnyQueryLike =
  | Query<string, ZeroSchema, any>
  | ((...args: any) => Query<string, ZeroSchema, any>)
  | CustomQueryPhantom;

export type QueryRowType<Q extends AnyQueryLike> = Q extends (
  ...args: any
) => Query<any, any, infer R>
  ? R
  : Q extends Query<any, any, infer R>
    ? R
    : Q extends CustomQueryPhantom
      ? Q['~']['$return']
      : never;

export type ZeRow<Q extends AnyQueryLike> = QueryRowType<Q>;

export type QueryResultType<Q extends AnyQueryLike> = Q extends
  | Query<string, ZeroSchema, any>
  | ((...args: any) => Query<string, ZeroSchema, any>)
  ? HumanReadable<QueryRowType<Q>>
  : Q extends CustomQueryPhantom
    ? HumanReadable<Q['~']['$return']>
    : never;

/**
 * A hybrid query that runs on both client and server.
 * Results are returned immediately from the client followed by authoritative
 * results from the server.
 *
 * Queries are transactional in that all queries update at once when a new transaction
 * has been committed on the client or server. No query results will reflect stale state.
 *
 * Queries are executed through the Zero instance methods:
 * - `zero.run(query)` - Execute once and return results
 * - `zero.materialize(query)` - Create a live view that updates automatically
 * - `zero.preload(query)` - Preload data into the cache
 *
 * The normal way to use a query is through your UI framework's bindings (e.g., `useQuery(query)`)
 * or within a custom mutator.
 *
 * Example:
 *
 * ```ts
 * const query = z.query.issue.where('status', 'open').limit(10);
 * const result = await z.run(query);
 * ```
 *
 * For more information on how to use queries, see the documentation:
 * https://zero.rocicorp.dev/docs/reading-data
 *
 * @typeParam TTable The name of the table being queried, must be a key of TSchema['tables']
 * @typeParam TSchema The database schema type extending ZeroSchema
 * @typeParam TReturn The return type of the query, defaults to PullRow<TTable, TSchema>
 */
export interface Query<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends ZeroSchema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
> {
  related<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
  ): Query<
    TTable,
    TSchema,
    AddSubreturn<
      TReturn,
      DestRow<TTable, TSchema, TRelationship>,
      TRelationship
    >
  >;
  related<
    TRelationship extends AvailableRelationships<TTable, TSchema>,
    TSub extends Query<string, TSchema, any>,
  >(
    relationship: TRelationship,
    cb: (
      q: Query<
        DestTableName<TTable, TSchema, TRelationship>,
        TSchema,
        DestRow<TTable, TSchema, TRelationship>
      >,
    ) => TSub,
  ): Query<
    TTable,
    TSchema,
    AddSubreturn<
      TReturn,
      TSub extends Query<string, TSchema, infer TSubReturn>
        ? TSubReturn
        : never,
      TRelationship
    >
  >;

  where<
    TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>,
    TOperator extends SimpleOperator,
  >(
    field: TSelector,
    op: TOperator,
    value:
      | GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, TOperator>
      | ParameterReference
      | undefined,
  ): Query<TTable, TSchema, TReturn>;
  where<
    TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>,
  >(
    field: TSelector,
    value:
      | GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, '='>
      | ParameterReference
      | undefined,
  ): Query<TTable, TSchema, TReturn>;
  where(
    expressionFactory: ExpressionFactory<TTable, TSchema>,
  ): Query<TTable, TSchema, TReturn>;

  whereExists(
    relationship: AvailableRelationships<TTable, TSchema>,
    options?: ExistsOptions,
  ): Query<TTable, TSchema, TReturn>;
  whereExists<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
    cb: (
      q: Query<DestTableName<TTable, TSchema, TRelationship>, TSchema>,
    ) => Query<string, TSchema>,
    options?: ExistsOptions,
  ): Query<TTable, TSchema, TReturn>;

  start(
    row: Partial<PullRow<TTable, TSchema>>,
    opts?: {inclusive: boolean},
  ): Query<TTable, TSchema, TReturn>;

  limit(limit: number): Query<TTable, TSchema, TReturn>;

  orderBy<TSelector extends Selector<PullTableSchema<TTable, TSchema>>>(
    field: TSelector,
    direction: 'asc' | 'desc',
  ): Query<TTable, TSchema, TReturn>;

  one(): Query<TTable, TSchema, TReturn | undefined>;

  /**
   * @deprecated Use {@linkcode RunOptions} with {@linkcode zero.run} instead.
   */
  run(options?: RunOptions): Promise<HumanReadable<TReturn>>;

  /**
   * @deprecated Use {@linkcode PreloadOptions} with {@linkcode zero.preload} instead.
   */
  preload(options?: PreloadOptions): {
    cleanup: () => void;
    complete: Promise<void>;
  };

  /**
   * @deprecated Use {@linkcode MaterializeOptions} with {@linkcode zero.materialize} instead.
   */
  materialize(ttl?: TTL): TypedView<HumanReadable<TReturn>>;
  /**
   * @deprecated Use {@linkcode MaterializeOptions} with {@linkcode zero.materialize} instead.
   */
  materialize<T>(
    factory: ViewFactory<TTable, TSchema, TReturn, T>,
    ttl?: TTL,
  ): T;
}

export type PreloadOptions = {
  /**
   * Time To Live. This is the amount of time to keep the rows associated with
   * this query after {@linkcode cleanup} has been called.
   */
  ttl?: TTL | undefined;
};

export type MaterializeOptions = PreloadOptions;

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
 *
 * The `ttl` option is used to specify the time to live for the query. This is the amount of
 * time to keep the rows associated with this query after the promise has resolved.
 */
export type RunOptions = {
  type: 'unknown' | 'complete';
  ttl?: TTL | undefined;
};

export const DEFAULT_RUN_OPTIONS_UNKNOWN = {
  type: 'unknown',
} as const;

export const DEFAULT_RUN_OPTIONS_COMPLETE = {
  type: 'complete',
} as const;

export type AnyQuery = Query<string, ZeroSchema, any>;
