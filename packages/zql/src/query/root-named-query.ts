import type {StandardSchemaV1} from '@standard-schema/spec';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {SimpleOperator} from '../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {ChainedQuery, type AnyChainQuery} from './chained-query.ts';
import type {DefineQueryFunc} from './define-query.ts';
import type {ExpressionFactory, ParameterReference} from './expression.ts';
import type {CustomQueryID} from './named.ts';
import {queryWithContext} from './query-internals.ts';
import type {AnyQuery} from './query.ts';
import {
  type AvailableRelationships,
  type DestTableName,
  type ExistsOptions,
  type GetFilterType,
  type NoCompoundTypeSelector,
  type PullRow,
  type PullTableSchema,
  type Query,
} from './query.ts';

/**
 * Root named query that has a name, input validation, and a function to execute.
 * This is the base query that doesn't chain from another query.
 */
export class RootNamedQuery<
  TName extends string,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TOutput extends ReadonlyJSONValue | undefined,
  TInput,
> implements Query<TSchema, TTable, TReturn, TContext>
{
  readonly #name: TName;
  readonly #input: TInput;
  readonly #func: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>;
  readonly #validator: StandardSchemaV1<TInput, TOutput> | undefined;
  #cachedQuery: Query<TSchema, TTable, TReturn, TContext> | undefined;

  constructor(
    name: TName,
    func: DefineQueryFunc<TSchema, TTable, TReturn, TContext, TOutput>,
    input: TInput,
    validator: StandardSchemaV1<TInput, TOutput> | undefined,
  ) {
    this.#name = name;
    this.#func = func;
    this.#input = input;
    this.#validator = validator;
  }

  withContext(ctx: TContext): Query<TSchema, TTable, TReturn, TContext> {
    if (this.#cachedQuery) {
      return this.#cachedQuery;
    }

    // This is a root query - call the function with the context
    let output: TOutput;
    if (!this.#validator) {
      // No validator, so input and output are the same
      output = this.#input as unknown as TOutput;
    } else {
      const result = this.#validator['~standard'].validate(this.#input);
      if (result instanceof Promise) {
        throw new Error(
          `Async validators are not supported. Query name ${this.#name}`,
        );
      }
      if (result.issues) {
        throw new Error(
          `Validation failed for query ${this.#name}: ${result.issues
            .map(issue => issue.message)
            .join(', ')}`,
        );
      }
      output = result.value;
    }

    // TODO: Refactor to deal with the name and args at a different abstraction
    // layer.
    this.#cachedQuery = queryWithContext(
      this.#func({ctx, args: output}),
      ctx,
    ).nameAndArgs(
      this.#name,
      this.#input === undefined ? [] : [this.#input as ReadonlyJSONValue],
    );
    return this.#cachedQuery;
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

  get customQueryID(): CustomQueryID {
    return {
      name: this.#name,
      args: this.#input === undefined ? [] : [this.#input as ReadonlyJSONValue],
    };
  }
}
