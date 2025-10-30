import type {SimpleOperator} from '../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {ExpressionFactory, ParameterReference} from './expression.ts';
import type {
  AnyQuery,
  AvailableRelationships,
  DestTableName,
  ExistsOptions,
  GetFilterType,
  NoCompoundTypeSelector,
  PullRow,
  PullTableSchema,
  Query,
} from './query.ts';

/**
 * Function type for chaining one query to another.
 */
type ChainQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn1,
  TReturn2,
  TContext,
> = (
  q: Query<TSchema, TTable, TReturn1, TContext>,
) => Query<TSchema, TTable, TReturn2, TContext>;

export type AnyChainQuery = ChainQuery<
  Schema,
  string,
  PullRow<string, Schema>,
  PullRow<string, Schema>,
  unknown
>;

/**
 * Chained query that applies a transformation function to a parent query.
 * This represents a query operation that builds on top of another query.
 */
export class ChainedQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
> implements Query<TSchema, TTable, TReturn, TContext>
{
  readonly #parent: {
    withContext(ctx: TContext): Query<TSchema, TTable, unknown>;
  };
  readonly #chainFn: AnyChainQuery;
  #q: Query<TSchema, TTable, TReturn> | undefined;

  constructor(
    parent: {withContext(ctx: TContext): Query<TSchema, TTable, unknown>},
    chainFn: AnyChainQuery,
  ) {
    this.#parent = parent;
    this.#chainFn = chainFn;
  }

  withContext(ctx: TContext): Query<TSchema, TTable, TReturn> {
    if (this.#q) {
      return this.#q;
    }

    // This is a chained query - get the parent query and apply the chain function
    const parentQuery = this.#parent.withContext(ctx);
    this.#q = this.#chainFn(parentQuery as AnyQuery) as Query<
      TSchema,
      TTable,
      TReturn
    >;
    return this.#q;
  }

  #withChain<TNewReturn>(
    fn: (
      q: Query<TSchema, TTable, TReturn>,
    ) => Query<TSchema, TTable, TNewReturn>,
  ): ChainedQuery<TSchema, TTable, TNewReturn, TContext> {
    return new ChainedQuery(
      this as {withContext(ctx: TContext): Query<TSchema, TTable, unknown>},
      fn as AnyChainQuery,
    );
  }

  // Query interface methods

  one(): ChainedQuery<TSchema, TTable, TReturn | undefined, TContext> {
    return this.#withChain(q => q.one());
  }

  whereExists<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
    options?: ExistsOptions,
  ): ChainedQuery<TSchema, TTable, TReturn, TContext>;
  whereExists<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
    cb: (
      q: Query<
        TSchema,
        DestTableName<TTable, TSchema, TRelationship>,
        TContext
      >,
    ) => Query<TSchema, string, TContext>,
    options?: ExistsOptions,
  ): ChainedQuery<TSchema, TTable, TReturn, TContext>;
  whereExists(
    relationship: AvailableRelationships<TTable, TSchema>,
    cbOrOptions?:
      | ((
          q: Query<TSchema, string, TContext>,
        ) => Query<TSchema, string, TContext>)
      | ExistsOptions,
    options?: ExistsOptions,
  ): ChainedQuery<TSchema, TTable, TReturn, TContext> {
    if (typeof cbOrOptions === 'function') {
      return this.#withChain(q =>
        q.whereExists(
          relationship as string,
          cbOrOptions as unknown as (q: AnyQuery) => AnyQuery,
          options,
        ),
      );
    }
    return this.#withChain(q =>
      q.whereExists(relationship as string, cbOrOptions),
    );
  }

  related<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
  ): ChainedQuery<TSchema, TTable, TReturn & Record<string, unknown>, TContext>;
  related<
    TRelationship extends AvailableRelationships<TTable, TSchema>,
    TSub extends Query<TSchema, string, unknown>,
  >(
    relationship: TRelationship,
    cb: (
      q: Query<
        TSchema,
        DestTableName<TTable, TSchema, TRelationship>,
        TContext
      >,
    ) => TSub,
  ): ChainedQuery<TSchema, TTable, TReturn & Record<string, unknown>, TContext>;
  related(
    relationship: AvailableRelationships<TTable, TSchema>,
    cb?: (
      q: Query<TSchema, string, TContext>,
    ) => Query<TSchema, string, TContext>,
  ): ChainedQuery<
    TSchema,
    TTable,
    TReturn & Record<string, unknown>,
    TContext
  > {
    if (cb) {
      return this.#withChain(q =>
        q.related(
          relationship as string,
          cb as unknown as (q: AnyQuery) => AnyQuery,
        ),
      ) as ChainedQuery<
        TSchema,
        TTable,
        TReturn & Record<string, unknown>,
        TContext
      >;
    }
    return this.#withChain(q =>
      q.related(relationship as string),
    ) as ChainedQuery<
      TSchema,
      TTable,
      TReturn & Record<string, unknown>,
      TContext
    >;
  }

  where<
    TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>,
    TOperator extends SimpleOperator,
  >(
    field: TSelector,
    op: TOperator,
    value:
      | GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, TOperator>
      | ParameterReference,
  ): ChainedQuery<TSchema, TTable, TReturn, TContext>;
  where<
    TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>,
  >(
    field: TSelector,
    value:
      | GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, '='>
      | ParameterReference,
  ): ChainedQuery<TSchema, TTable, TReturn, TContext>;
  where(
    expressionFactory: ExpressionFactory<TSchema, TTable>,
  ): ChainedQuery<TSchema, TTable, TReturn, TContext>;
  where(
    fieldOrExpressionFactory:
      | NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>
      | ExpressionFactory<TSchema, TTable>,
    opOrValue?: unknown,
    value?: unknown,
  ): ChainedQuery<TSchema, TTable, TReturn, TContext> {
    if (typeof fieldOrExpressionFactory === 'function') {
      return this.#withChain(q => q.where(fieldOrExpressionFactory));
    }
    if (value !== undefined) {
      return this.#withChain(q =>
        // Cast to bypass TypeScript's strict type checking - this proxy method needs runtime flexibility
        (
          q as unknown as {
            where(
              field: unknown,
              op: unknown,
              val: unknown,
            ): Query<TSchema, TTable, TReturn>;
          }
        ).where(fieldOrExpressionFactory, opOrValue, value),
      );
    }
    return this.#withChain(q =>
      // Cast to bypass TypeScript's strict type checking - this proxy method needs runtime flexibility
      (
        q as unknown as {
          where(field: unknown, val: unknown): Query<TSchema, TTable, TReturn>;
        }
      ).where(fieldOrExpressionFactory, opOrValue),
    );
  }

  start(
    row: Partial<PullRow<TTable, TSchema>>,
    opts?: {inclusive: boolean},
  ): ChainedQuery<TSchema, TTable, TReturn, TContext> {
    return this.#withChain(q => q.start(row, opts));
  }

  limit(limit: number): ChainedQuery<TSchema, TTable, TReturn, TContext> {
    return this.#withChain(q => q.limit(limit));
  }

  orderBy<TSelector extends keyof PullTableSchema<TTable, TSchema>['columns']>(
    field: TSelector,
    direction: 'asc' | 'desc',
  ): ChainedQuery<TSchema, TTable, TReturn, TContext> {
    return this.#withChain(q => q.orderBy(field as string, direction));
  }
}
