import type {SimpleOperator} from '../../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import type {ExpressionFactory, ParameterReference} from '../expression.ts';
import type {
  AvailableRelationships,
  CoreQuery,
  DestTableName,
  ExistsOptions,
  GetFilterType,
  NoCompoundTypeSelector,
  PullRow,
  PullTableSchema,
  Query,
} from '../query.ts';

/**
 * Interface for types that can resolve with context
 */
export interface WithContext<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
> {
  /**
   * Resolve this query implementation with the provided context. This is the
   * main method that converts the query into an executable Query.
   *
   * If the validator returns a Promise (ie async validation), this method will
   * throw.
   */
  withContext(ctx: TContext): Query<TSchema, TTable, TReturn>;
}

/**
 * Interface for chained query implementation
 */
export interface ChainedQueryInterface<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
> extends QueryImplementation<TSchema, TTable, TReturn, TContext> {}

/**
 * Common interface for both root named queries and chained queries.
 * This provides the core Query interface methods that both implementations share.
 */
export interface QueryImplementation<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
> extends CoreQuery<TSchema, TTable, TReturn>,
    WithContext<TSchema, TTable, TReturn, TContext> {
  // Query chain methods - these return new query implementations

  one(): QueryImplementation<TSchema, TTable, TReturn | undefined, TContext>;

  whereExists<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
    options?: ExistsOptions,
  ): QueryImplementation<TSchema, TTable, TReturn, TContext>;

  whereExists<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
    cb: (
      q: CoreQuery<
        TSchema,
        DestTableName<TTable, TSchema, TRelationship>,
        TContext
      >,
    ) => CoreQuery<TSchema, string, TContext>,
    options?: ExistsOptions,
  ): QueryImplementation<TSchema, TTable, TReturn, TContext>;

  related<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
  ): QueryImplementation<
    TSchema,
    TTable,
    TReturn & Record<string, unknown>,
    TContext
  >;

  related<
    TRelationship extends AvailableRelationships<TTable, TSchema>,
    TSub extends CoreQuery<TSchema, string, unknown>,
  >(
    relationship: TRelationship,
    cb: (
      q: CoreQuery<
        TSchema,
        DestTableName<TTable, TSchema, TRelationship>,
        TContext
      >,
    ) => TSub,
  ): QueryImplementation<
    TSchema,
    TTable,
    TReturn & Record<string, unknown>,
    TContext
  >;

  where<
    TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>,
    TOperator extends SimpleOperator,
  >(
    field: TSelector,
    op: TOperator,
    value:
      | GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, TOperator>
      | ParameterReference,
  ): QueryImplementation<TSchema, TTable, TReturn, TContext>;

  where<
    TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>,
  >(
    field: TSelector,
    value:
      | GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, '='>
      | ParameterReference,
  ): QueryImplementation<TSchema, TTable, TReturn, TContext>;

  where(
    expressionFactory: ExpressionFactory<TSchema, TTable>,
  ): QueryImplementation<TSchema, TTable, TReturn, TContext>;

  start(
    row: Partial<PullRow<TTable, TSchema>>,
    opts?: {inclusive: boolean},
  ): QueryImplementation<TSchema, TTable, TReturn, TContext>;

  limit(limit: number): QueryImplementation<TSchema, TTable, TReturn, TContext>;

  orderBy<TSelector extends keyof PullTableSchema<TTable, TSchema>['columns']>(
    field: TSelector,
    direction: 'asc' | 'desc',
  ): QueryImplementation<TSchema, TTable, TReturn, TContext>;
}

/**
 * Function type for root query functions that take context and args.
 */
export type Func<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TArgs,
> = (options: {ctx: TContext; args: TArgs}) => Query<TSchema, TTable, TReturn>;

/**
 * Function type for chaining one query to another.
 */
export type ChainQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn1,
  TReturn2,
> = (q: Query<TSchema, TTable, TReturn1>) => Query<TSchema, TTable, TReturn2>;

export type AnyChainQuery = ChainQuery<
  Schema,
  string,
  PullRow<string, Schema>,
  PullRow<string, Schema>
>;
