import type {StandardSchemaV1} from '@standard-schema/spec';
import type {ReadonlyJSONValue} from '../../../../shared/src/json.ts';
import type {SimpleOperator} from '../../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import type {ExpressionFactory, ParameterReference} from '../expression.ts';
import type {CustomQueryID} from '../named.ts';
import type {AnyQuery} from '../query-impl.ts';
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
import {ChainedQuery} from './chained-query.ts';
import type {AnyChainQuery, Func, QueryImplementation} from './types.ts';

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
  >
  implements
    QueryImplementation<TSchema, TTable, TReturn, TContext>,
    QueryInternals
{
  readonly #name: TName;
  readonly #input: TInput;
  readonly #func: Func<TSchema, TTable, TReturn, TContext, TOutput>;
  readonly #validator: StandardSchemaV1<TInput, TOutput> | undefined;
  #q: Query<TSchema, TTable, TReturn> | undefined;

  constructor(
    name: TName,
    func: Func<TSchema, TTable, TReturn, TContext, TOutput>,
    input: TInput,
    validator: StandardSchemaV1<TInput, TOutput> | undefined,
  ) {
    this.#name = name;
    this.#func = func;
    this.#input = input;
    this.#validator = validator;
  }

  withContext(ctx: TContext): Query<TSchema, TTable, TReturn> {
    if (this.#q) {
      return this.#q;
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
    this.#q = (this.#func({ctx, args: output}) as AnyQuery).nameAndArgs(
      this.#name,
      this.#input === undefined ? [] : [this.#input as ReadonlyJSONValue],
    ) as Query<TSchema, TTable, TReturn>;
    return this.#q;
  }

  #withChain<TNewReturn>(
    fn: (
      q: Query<TSchema, TTable, TReturn>,
    ) => Query<TSchema, TTable, TNewReturn>,
  ): QueryImplementation<TSchema, TTable, TNewReturn, TContext> {
    return new ChainedQuery(
      this as QueryImplementation<TSchema, TTable, unknown, TContext>,
      fn as AnyChainQuery,
    );
  }

  // Query interface methods

  one(): QueryImplementation<TSchema, TTable, TReturn | undefined, TContext> {
    return this.#withChain(q => q.one());
  }

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
  whereExists(
    relationship: AvailableRelationships<TTable, TSchema>,
    cbOrOptions?:
      | ((
          q: CoreQuery<TSchema, string, TContext>,
        ) => CoreQuery<TSchema, string, TContext>)
      | ExistsOptions,
    options?: ExistsOptions,
  ): QueryImplementation<TSchema, TTable, TReturn, TContext> {
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
  related(
    relationship: AvailableRelationships<TTable, TSchema>,
    cb?: (
      q: CoreQuery<TSchema, string, TContext>,
    ) => CoreQuery<TSchema, string, TContext>,
  ): QueryImplementation<
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
      ) as QueryImplementation<
        TSchema,
        TTable,
        TReturn & Record<string, unknown>,
        TContext
      >;
    }
    return this.#withChain(q =>
      q.related(relationship as string),
    ) as QueryImplementation<
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
  where(
    fieldOrExpressionFactory:
      | NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>
      | ExpressionFactory<TSchema, TTable>,
    opOrValue?: unknown,
    value?: unknown,
  ): QueryImplementation<TSchema, TTable, TReturn, TContext> {
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
  ): QueryImplementation<TSchema, TTable, TReturn, TContext> {
    return this.#withChain(q => q.start(row, opts));
  }

  limit(
    limit: number,
  ): QueryImplementation<TSchema, TTable, TReturn, TContext> {
    return this.#withChain(q => q.limit(limit));
  }

  orderBy<TSelector extends keyof PullTableSchema<TTable, TSchema>['columns']>(
    field: TSelector,
    direction: 'asc' | 'desc',
  ): QueryImplementation<TSchema, TTable, TReturn, TContext> {
    return this.#withChain(q => q.orderBy(field as string, direction));
  }

  // QueryInternals interface methods

  get customQueryID(): CustomQueryID {
    return {
      name: this.#name,
      args: this.#input === undefined ? [] : [this.#input as ReadonlyJSONValue],
    };
  }
}

export interface QueryInternals {
  readonly customQueryID: CustomQueryID;
}
