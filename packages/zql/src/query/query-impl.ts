/* eslint-disable @typescript-eslint/no-explicit-any */
import {resolver} from '@rocicorp/resolver';
import {assert} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {Writable} from '../../../shared/src/writable.ts';
import type {
  AST,
  CompoundKey,
  Condition,
  Ordering,
  Parameter,
  SimpleOperator,
  System,
} from '../../../zero-protocol/src/ast.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import type {Row as IVMRow} from '../../../zero-protocol/src/data.ts';
import {
  hashOfAST,
  hashOfNameAndArgs,
} from '../../../zero-protocol/src/query-hash.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  isOneHop,
  isTwoHop,
  type TableSchema,
} from '../../../zero-schema/src/table-schema.ts';
import {buildPipeline} from '../builder/builder.ts';
import {NotImplementedError} from '../error.ts';
import {ArrayView} from '../ivm/array-view.ts';
import type {Input} from '../ivm/operator.ts';
import type {Format, ViewFactory} from '../ivm/view.ts';
import {assertNoNotExists} from './assert-no-not-exists.ts';
import {
  and,
  cmp,
  ExpressionBuilder,
  simplifyCondition,
  type ExpressionFactory,
} from './expression.ts';
import type {CustomQueryID} from './named.ts';
import type {GotCallback, QueryDelegate} from './query-delegate.ts';
import {asQueryInternals, type QueryInternals} from './query-internals.ts';
import {
  NoContext,
  type ExistsOptions,
  type GetFilterType,
  type HumanReadable,
  type MaterializeOptions,
  type PreloadOptions,
  type PullRow,
  type Query,
  type RunOptions,
} from './query.ts';
import {DEFAULT_PRELOAD_TTL_MS, DEFAULT_TTL_MS, type TTL} from './ttl.ts';
import type {TypedView} from './typed-view.ts';

export type AnyQuery = Query<Schema, string, any, any>;

export function materialize<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
>(
  query: Query<TSchema, TTable, TReturn, TContext>,
  delegate: QueryDelegate,
  ctx: TContext,
  factoryOrOptions?:
    | ViewFactory<TSchema, TTable, TReturn, TContext, unknown>
    | MaterializeOptions,
  maybeOptions?: MaterializeOptions,
) {
  const qi = withContext(query, ctx);
  return materializeImpl(qi, delegate, factoryOrOptions, maybeOptions);
}

export function preload<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
>(
  query: Query<TSchema, TTable, TReturn, TContext>,
  delegate: QueryDelegate,
  ctx: TContext,
  options?: PreloadOptions,
): {
  cleanup: () => void;
  complete: Promise<void>;
} {
  const qi = withContext(query, ctx);
  return preloadImpl(qi, delegate, options);
}

export function run<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
>(
  query: Query<TSchema, TTable, TReturn, TContext>,
  delegate: QueryDelegate,
  ctx: TContext,
  options?: RunOptions,
): Promise<HumanReadable<TReturn>> {
  const qi = withContext(query, ctx);
  return runImpl(qi, delegate, options);
}

const astSymbol = Symbol();

export function ast(query: AnyQuery): AST {
  return (query as AbstractQuery<Schema, string, unknown, unknown>)[astSymbol];
}

export function newQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
>(
  delegate: QueryDelegate | undefined,
  schema: TSchema,
  table: TTable,
): Query<TSchema, TTable, TReturn> {
  return new QueryImpl(
    delegate,
    schema,
    table,
    {table},
    defaultFormat,
    undefined,
  );
}

export function staticParam(
  anchorClass: 'authData' | 'preMutationRow',
  field: string | string[],
): Parameter {
  return {
    type: 'static',
    anchor: anchorClass,
    // for backwards compatibility
    field: field.length === 1 ? field[0] : field,
  };
}

export const SUBQ_PREFIX = 'zsubq_';

export const defaultFormat = {singular: false, relationships: {}} as const;

// export const newQuerySymbol = Symbol();

export abstract class AbstractQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
  TContext = NoContext,
> implements Query<TSchema, TTable, TReturn, TContext>
{
  readonly #schema: TSchema;
  protected readonly _delegate: QueryDelegate | undefined;
  readonly #tableName: TTable;
  readonly _ast: AST;
  readonly format: Format;
  #hash: string = '';
  readonly #system: System;
  readonly #currentJunction: string | undefined;
  readonly customQueryID: CustomQueryID | undefined;

  constructor(
    delegate: QueryDelegate | undefined,
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
    system: System,
    customQueryID: CustomQueryID | undefined,
    currentJunction?: string,
  ) {
    this.#schema = schema;
    this._delegate = delegate;
    this.#tableName = tableName;
    this._ast = ast;
    this.format = format;
    this.#system = system;
    this.#currentJunction = currentJunction;
    this.customQueryID = customQueryID;
  }

  // withDelegate(
  //   delegate: QueryDelegate,
  // ): Query<TSchema, TTable, TReturn, TContext> {
  //   return this._newQuerySymbol(
  //     delegate,
  //     this.#schema,
  //     this.#tableName,
  //     this._ast,
  //     this.format,
  //     this.customQueryID,
  //     this.#currentJunction,

  //   );
  // }

  withContext(
    _ctx: TContext,
  ): QueryInternals<TSchema, TTable, TReturn, TContext> {
    return this as QueryInternals<TSchema, TTable, TReturn, TContext>;
  }

  nameAndArgs(
    name: string,
    args: ReadonlyArray<ReadonlyJSONValue>,
  ): Query<TSchema, TTable, TReturn, TContext> {
    return this._newQuerySymbol(
      this._delegate,
      this.#schema,
      this.#tableName,
      this._ast,
      this.format,
      {
        name,
        args: args as ReadonlyArray<ReadonlyJSONValue>,
      },
      this.#currentJunction,
    );
  }

  get [astSymbol](): AST {
    return this._ast;
  }

  get ast() {
    return this.#completeAst();
  }

  hash(): string {
    if (!this.#hash) {
      this.#hash = hashOfAST(this.#completeAst());
    }
    return this.#hash;
  }

  // TODO(arv): Put this in the delegate?
  protected abstract _newQuerySymbol<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TContext = NoContext,
  >(
    delegate: QueryDelegate | undefined,
    schema: TSchema,
    table: TTable,
    ast: AST,
    format: Format,
    customQueryID: CustomQueryID | undefined,
    currentJunction: string | undefined,
  ): AbstractQuery<TSchema, TTable, TReturn, TContext>;

  one = (): Query<TSchema, TTable, TReturn | undefined, TContext> =>
    this._newQuerySymbol(
      this._delegate,
      this.#schema,
      this.#tableName,
      {
        ...this._ast,
        limit: 1,
      },
      {
        ...this.format,
        singular: true,
      },
      this.customQueryID,
      this.#currentJunction,
    );

  whereExists = (
    relationship: string,
    cbOrOptions?: ((q: AnyQuery) => AnyQuery) | ExistsOptions,
    options?: ExistsOptions,
  ): Query<TSchema, TTable, TReturn, TContext> => {
    const cb = typeof cbOrOptions === 'function' ? cbOrOptions : undefined;
    const opts = typeof cbOrOptions === 'function' ? options : cbOrOptions;
    const flipped = opts?.flip ?? false;
    return this.where(({exists}) => exists(relationship, cb, {flip: flipped}));
  };

  related = (
    relationship: string,
    cb?: (q: AnyQuery) => AnyQuery,
  ): Query<Schema, string, any, TContext> => {
    if (relationship.startsWith(SUBQ_PREFIX)) {
      throw new Error(
        `Relationship names may not start with "${SUBQ_PREFIX}". That is a reserved prefix.`,
      );
    }
    cb = cb ?? (q => q);

    const related = this.#schema.relationships[this.#tableName][relationship];
    assert(related, 'Invalid relationship');
    if (isOneHop(related)) {
      const {destSchema, destField, sourceField, cardinality} = related[0];
      const q: AnyQuery = this._newQuerySymbol(
        this._delegate,
        this.#schema,
        destSchema,
        {
          table: destSchema,
          alias: relationship,
        },
        {
          relationships: {},
          singular: cardinality === 'one',
        },
        this.customQueryID,
        undefined,
      ) as AnyQuery;
      // Intentionally not setting to `one` as it is a perf degradation
      // and the user should not be making the mistake of setting cardinality to
      // `one` when it is actually not.
      // if (cardinality === 'one') {
      //   q = q.one();
      // }
      const sq = cb(q) as AbstractQuery<Schema, string, unknown, unknown>;
      assert(
        isCompoundKey(sourceField),
        'The source of a relationship must specify at last 1 field',
      );
      assert(
        isCompoundKey(destField),
        'The destination of a relationship must specify at last 1 field',
      );
      assert(
        sourceField.length === destField.length,
        'The source and destination of a relationship must have the same number of fields',
      );

      return this._newQuerySymbol(
        this._delegate,
        this.#schema,
        this.#tableName,
        {
          ...this._ast,
          related: [
            ...(this._ast.related ?? []),
            {
              system: this.#system,
              correlation: {
                parentField: sourceField,
                childField: destField,
              },
              subquery: addPrimaryKeysToAst(
                this.#schema.tables[destSchema],
                sq._ast,
              ),
            },
          ],
        },
        {
          ...this.format,
          relationships: {
            ...this.format.relationships,
            [relationship]: sq.format,
          },
        },
        this.customQueryID,
        this.#currentJunction,
      ) as AnyQuery;
    }

    if (isTwoHop(related)) {
      const [firstRelation, secondRelation] = related;
      const {destSchema} = secondRelation;
      const junctionSchema = firstRelation.destSchema;
      const sq = asQueryInternals(
        cb(
          this._newQuerySymbol(
            this._delegate,
            this.#schema,
            destSchema,
            {
              table: destSchema,
              alias: relationship,
            },
            {
              relationships: {},
              singular: secondRelation.cardinality === 'one',
            },
            this.customQueryID,
            relationship,
          ) as AnyQuery,
        ),
      );

      assert(isCompoundKey(firstRelation.sourceField), 'Invalid relationship');
      assert(isCompoundKey(firstRelation.destField), 'Invalid relationship');
      assert(isCompoundKey(secondRelation.sourceField), 'Invalid relationship');
      assert(isCompoundKey(secondRelation.destField), 'Invalid relationship');

      return this._newQuerySymbol(
        this._delegate,
        this.#schema,
        this.#tableName,
        {
          ...this._ast,
          related: [
            ...(this._ast.related ?? []),
            {
              system: this.#system,
              correlation: {
                parentField: firstRelation.sourceField,
                childField: firstRelation.destField,
              },
              hidden: true,
              subquery: {
                table: junctionSchema,
                alias: relationship,
                orderBy: addPrimaryKeys(
                  this.#schema.tables[junctionSchema],
                  undefined,
                ),
                related: [
                  {
                    system: this.#system,
                    correlation: {
                      parentField: secondRelation.sourceField,
                      childField: secondRelation.destField,
                    },
                    subquery: addPrimaryKeysToAst(
                      this.#schema.tables[destSchema],
                      sq.ast,
                    ),
                  },
                ],
              },
            },
          ],
        },
        {
          ...this.format,
          relationships: {
            ...this.format.relationships,
            [relationship]: sq.format,
          },
        },
        this.customQueryID,
        this.#currentJunction,
      ) as AnyQuery;
    }

    throw new Error(`Invalid relationship ${relationship}`);
  };

  where = (
    fieldOrExpressionFactory: string | ExpressionFactory<TSchema, TTable>,
    opOrValue?: SimpleOperator | GetFilterType<any, any, any> | Parameter,
    value?: GetFilterType<any, any, any> | Parameter,
  ): Query<TSchema, TTable, TReturn, TContext> => {
    let cond: Condition;

    if (typeof fieldOrExpressionFactory === 'function') {
      cond = fieldOrExpressionFactory(
        new ExpressionBuilder(this._exists) as ExpressionBuilder<
          TSchema,
          TTable
        >,
      );
    } else {
      assert(opOrValue !== undefined, 'Invalid condition');
      cond = cmp(fieldOrExpressionFactory, opOrValue, value);
    }

    const existingWhere = this._ast.where;
    if (existingWhere) {
      cond = and(existingWhere, cond);
    }

    const where = simplifyCondition(cond);

    if (this.#system === 'client') {
      // We need to do this after the DNF since the DNF conversion might change
      // an EXISTS to a NOT EXISTS condition (and vice versa).
      assertNoNotExists(where);
    }

    return this._newQuerySymbol(
      this._delegate,
      this.#schema,
      this.#tableName,
      {
        ...this._ast,
        where,
      },
      this.format,
      this.customQueryID,
      this.#currentJunction,
    );
  };

  start = (
    row: Partial<PullRow<TTable, TSchema>>,
    opts?: {inclusive: boolean},
  ): Query<TSchema, TTable, TReturn, TContext> =>
    this._newQuerySymbol(
      this._delegate,
      this.#schema,
      this.#tableName,
      {
        ...this._ast,
        start: {
          row,
          exclusive: !opts?.inclusive,
        },
      },
      this.format,
      this.customQueryID,
      this.#currentJunction,
    );

  limit = (limit: number): Query<TSchema, TTable, TReturn, TContext> => {
    if (limit < 0) {
      throw new Error('Limit must be non-negative');
    }
    if ((limit | 0) !== limit) {
      throw new Error('Limit must be an integer');
    }
    if (this.#currentJunction) {
      throw new NotImplementedError(
        'Limit is not supported in junction relationships yet. Junction relationship being limited: ' +
          this.#currentJunction,
      );
    }

    return this._newQuerySymbol(
      this._delegate,
      this.#schema,
      this.#tableName,
      {
        ...this._ast,
        limit,
      },
      this.format,
      this.customQueryID,
      this.#currentJunction,
    );
  };

  orderBy = <TSelector extends keyof TSchema['tables'][TTable]['columns']>(
    field: TSelector,
    direction: 'asc' | 'desc',
  ): Query<TSchema, TTable, TReturn, TContext> => {
    if (this.#currentJunction) {
      throw new NotImplementedError(
        'Order by is not supported in junction relationships yet. Junction relationship being ordered: ' +
          this.#currentJunction,
      );
    }
    return this._newQuerySymbol(
      this._delegate,
      this.#schema,
      this.#tableName,
      {
        ...this._ast,
        orderBy: [...(this._ast.orderBy ?? []), [field as string, direction]],
      },
      this.format,
      this.customQueryID,
      this.#currentJunction,
    );
  };

  protected _exists = (
    relationship: string,
    cb: ((query: AnyQuery) => AnyQuery) | undefined,
    options?: ExistsOptions,
  ): Condition => {
    cb = cb ?? (q => q);
    const flip = options?.flip ?? false;
    const related = this.#schema.relationships[this.#tableName][relationship];
    assert(related, 'Invalid relationship');

    if (isOneHop(related)) {
      const {destSchema, sourceField, destField} = related[0];
      assert(isCompoundKey(sourceField), 'Invalid relationship');
      assert(isCompoundKey(destField), 'Invalid relationship');

      const sq = cb(
        this._newQuerySymbol(
          this._delegate,
          this.#schema,
          destSchema,
          {
            table: destSchema,
            alias: `${SUBQ_PREFIX}${relationship}`,
          },
          defaultFormat,
          this.customQueryID,
          undefined,
        ) as AnyQuery,
      ) as QueryImpl<Schema, string, unknown, unknown>;
      return {
        type: 'correlatedSubquery',
        related: {
          system: this.#system,
          correlation: {
            parentField: sourceField,
            childField: destField,
          },
          subquery: addPrimaryKeysToAst(
            this.#schema.tables[destSchema],
            sq._ast,
          ),
        },
        op: 'EXISTS',
        flip,
      };
    }

    if (isTwoHop(related)) {
      const [firstRelation, secondRelation] = related;
      assert(isCompoundKey(firstRelation.sourceField), 'Invalid relationship');
      assert(isCompoundKey(firstRelation.destField), 'Invalid relationship');
      assert(isCompoundKey(secondRelation.sourceField), 'Invalid relationship');
      assert(isCompoundKey(secondRelation.destField), 'Invalid relationship');
      const {destSchema} = secondRelation;
      const junctionSchema = firstRelation.destSchema;
      const queryToDest = cb(
        this._newQuerySymbol(
          this._delegate,
          this.#schema,
          destSchema,
          {
            table: destSchema,
            alias: `${SUBQ_PREFIX}zhidden_${relationship}`,
          },
          defaultFormat,
          this.customQueryID,
          relationship,
        ) as AnyQuery,
      );

      return {
        type: 'correlatedSubquery',
        related: {
          system: this.#system,
          correlation: {
            parentField: firstRelation.sourceField,
            childField: firstRelation.destField,
          },
          subquery: {
            table: junctionSchema,
            alias: `${SUBQ_PREFIX}${relationship}`,
            orderBy: addPrimaryKeys(
              this.#schema.tables[junctionSchema],
              undefined,
            ),
            where: {
              type: 'correlatedSubquery',
              related: {
                system: this.#system,
                correlation: {
                  parentField: secondRelation.sourceField,
                  childField: secondRelation.destField,
                },

                subquery: addPrimaryKeysToAst(
                  this.#schema.tables[destSchema],
                  (queryToDest as QueryImpl<Schema, string, unknown, unknown>)
                    ._ast,
                ),
              },
              op: 'EXISTS',
              flip,
            },
          },
        },
        op: 'EXISTS',
        flip,
      };
    }

    throw new Error(`Invalid relationship ${relationship}`);
  };

  #completedAST: AST | undefined;

  get completedAST(): AST {
    return this.#completeAst();
  }

  #completeAst(): AST {
    if (!this.#completedAST) {
      const finalOrderBy = addPrimaryKeys(
        this.#schema.tables[this.#tableName],
        this._ast.orderBy,
      );
      if (this._ast.start) {
        const {row} = this._ast.start;
        const narrowedRow: Writable<IVMRow> = {};
        for (const [field] of finalOrderBy) {
          narrowedRow[field] = row[field];
        }
        this.#completedAST = {
          ...this._ast,
          start: {
            ...this._ast.start,
            row: narrowedRow,
          },
          orderBy: finalOrderBy,
        };
      } else {
        this.#completedAST = {
          ...this._ast,
          orderBy: addPrimaryKeys(
            this.#schema.tables[this.#tableName],
            this._ast.orderBy,
          ),
        };
      }
    }
    return this.#completedAST;
  }

  abstract materialize(
    options?: MaterializeOptions,
  ): TypedView<HumanReadable<TReturn>>;
  abstract materialize<T>(
    factory: ViewFactory<TSchema, TTable, TReturn, TContext, T>,
    options?: MaterializeOptions,
  ): T;

  abstract run(options?: RunOptions): Promise<HumanReadable<TReturn>>;

  abstract preload(options?: PreloadOptions): {
    cleanup: () => void;
    complete: Promise<void>;
  };
}

// export function completedAST(
//   q: Query<Schema, string, PullRow<string, Schema>>,
// ) {
//   return (q as QueryImpl<Schema, string, PullRow<string, Schema>>)[
//     completedAstSymbol
//   ];
// }

export function materializeImpl<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  T,
>(
  query: QueryInternals<TSchema, TTable, TReturn, TContext>,
  delegate: QueryDelegate,
  // ast: AST,
  // format: Format,
  // customQueryID: CustomQueryID | undefined,
  // queryHash: string,
  factoryOrOptions?:
    | ViewFactory<TSchema, TTable, TReturn, TContext, T>
    | MaterializeOptions,
  maybeOptions?: MaterializeOptions,
): T {
  let factory: ViewFactory<TSchema, TTable, TReturn, TContext, T> | undefined;
  let ttl: TTL = DEFAULT_TTL_MS;

  const ast = query.completedAST;
  const format = query.format;
  const customQueryID = query.customQueryID;
  const queryHash = query.hash();

  if (typeof factoryOrOptions === 'function') {
    factory = factoryOrOptions;
    ttl = maybeOptions?.ttl ?? DEFAULT_TTL_MS;
  } else if (factoryOrOptions) {
    ttl = factoryOrOptions.ttl ?? DEFAULT_TTL_MS;
  }

  const queryID = customQueryID
    ? hashOfNameAndArgs(customQueryID.name, customQueryID.args)
    : queryHash;
  const queryCompleteResolver = resolver<true>();
  let queryComplete: boolean | ErroredQuery = delegate.defaultQueryComplete;
  const updateTTL = customQueryID
    ? (newTTL: TTL) => delegate.updateCustomQuery(customQueryID, newTTL)
    : (newTTL: TTL) => delegate.updateServerQuery(ast, newTTL);

  const gotCallback: GotCallback = (got, error) => {
    if (error) {
      queryCompleteResolver.reject(error);
      queryComplete = error;
      return;
    }

    if (got) {
      delegate.addMetric(
        'query-materialization-end-to-end',
        performance.now() - t0,
        queryID,
        ast,
      );
      queryComplete = true;
      queryCompleteResolver.resolve(true);
    }
  };

  let removeCommitObserver: (() => void) | undefined;
  const onDestroy = () => {
    input.destroy();
    removeCommitObserver?.();
    removeAddedQuery();
  };

  const t0 = performance.now();

  const removeAddedQuery = customQueryID
    ? delegate.addCustomQuery(ast, customQueryID, ttl, gotCallback)
    : delegate.addServerQuery(ast, ttl, gotCallback);

  const input = buildPipeline(ast, delegate, queryID);

  const view = delegate.batchViewUpdates(() =>
    (factory ?? arrayViewFactory)(
      query,
      input,
      format,
      onDestroy,
      cb => {
        removeCommitObserver = delegate.onTransactionCommit(cb);
      },
      queryComplete || queryCompleteResolver.promise,
      updateTTL,
    ),
  );

  delegate.addMetric(
    'query-materialization-client',
    performance.now() - t0,
    queryID,
  );

  return view as T;
}

function runImpl<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
>(
  query: QueryInternals<TSchema, TTable, TReturn, TContext>,
  delegate: QueryDelegate,
  options?: RunOptions,
): Promise<HumanReadable<TReturn>> {
  delegate.assertValidRunOptions(options);
  const v: TypedView<HumanReadable<TReturn>> = materializeImpl(
    query,
    delegate,
    {ttl: options?.ttl},
  );
  if (options?.type === 'complete') {
    return new Promise(resolve => {
      v.addListener((data, type) => {
        if (type === 'complete') {
          v.destroy();
          resolve(data as HumanReadable<TReturn>);
        } else if (type === 'error') {
          v.destroy();
          resolve(Promise.reject(data));
        }
      });
    });
  }

  options?.type satisfies 'unknown' | undefined;

  const ret = v.data;
  v.destroy();
  return Promise.resolve(ret);
}

export function preloadImpl<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
>(
  query: QueryInternals<TSchema, TTable, TReturn, TContext>,
  delegate: QueryDelegate,
  options?: PreloadOptions,
): {
  cleanup: () => void;
  complete: Promise<void>;
} {
  const ttl = options?.ttl ?? DEFAULT_PRELOAD_TTL_MS;
  const {resolve, promise: complete} = resolver<void>();
  const {customQueryID, completedAST} = query;
  if (customQueryID) {
    const cleanup = delegate.addCustomQuery(
      completedAST,
      customQueryID,
      ttl,
      got => {
        if (got) {
          resolve();
        }
      },
    );
    return {
      cleanup,
      complete,
    };
  }

  const cleanup = delegate.addServerQuery(completedAST, ttl, got => {
    if (got) {
      resolve();
    }
  });
  return {
    cleanup,
    complete,
  };
}

export class QueryImpl<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
  TContext = NoContext,
> extends AbstractQuery<TSchema, TTable, TReturn, TContext> {
  readonly #system: System;

  constructor(
    delegate: QueryDelegate | undefined,
    schema: TSchema,
    tableName: TTable,
    ast: AST = {table: tableName},
    format: Format = defaultFormat,
    system: System = 'client',
    customQueryID?: CustomQueryID,
    currentJunction?: string,
  ) {
    super(
      delegate,
      schema,
      tableName,
      ast,
      format,
      system,
      customQueryID,
      currentJunction,
    );
    this.#system = system;
  }

  protected _newQuerySymbol<
    TSchema extends Schema,
    TTable extends string,
    TReturn,
    TContext,
  >(
    delegate: QueryDelegate | undefined,
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
    customQueryID: CustomQueryID | undefined,
    currentJunction: string | undefined,
  ): QueryImpl<TSchema, TTable, TReturn, TContext> {
    return new QueryImpl(
      delegate,
      schema,
      tableName,
      ast,
      format,
      this.#system,
      customQueryID,
      currentJunction,
    );
  }

  materialize<T>(
    factoryOrOptions?:
      | ViewFactory<TSchema, TTable, TReturn, TContext, T>
      | MaterializeOptions,
    maybeOptions?: MaterializeOptions,
  ): T {
    const delegate = must(
      this._delegate,
      'materialize requires a query delegate to be set',
    );
    return materializeImpl(this, delegate, factoryOrOptions, maybeOptions);
  }

  run(options?: RunOptions): Promise<HumanReadable<TReturn>> {
    const delegate = must(
      this._delegate,
      'run requires a query delegate to be set',
    );
    return runImpl(this, delegate, options);
  }

  preload(options?: PreloadOptions): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    const delegate = must(
      this._delegate,
      'preload requires a query delegate to be set',
    );
    return preloadImpl(this, delegate, options);
  }
}

function addPrimaryKeys(
  schema: TableSchema,
  orderBy: Ordering | undefined,
): Ordering {
  orderBy = orderBy ?? [];
  const {primaryKey} = schema;
  const primaryKeysToAdd = new Set(primaryKey);

  for (const [field] of orderBy) {
    primaryKeysToAdd.delete(field);
  }

  if (primaryKeysToAdd.size === 0) {
    return orderBy;
  }

  return [
    ...orderBy,
    ...[...primaryKeysToAdd].map(key => [key, 'asc'] as [string, 'asc']),
  ];
}

function addPrimaryKeysToAst(schema: TableSchema, ast: AST): AST {
  return {
    ...ast,
    orderBy: addPrimaryKeys(schema, ast.orderBy),
  };
}

function arrayViewFactory<
  TSchema extends Schema,
  TTable extends string,
  TReturn,
  TContext,
>(
  _query: QueryInternals<TSchema, TTable, TReturn, TContext>,
  input: Input,
  format: Format,
  onDestroy: () => void,
  onTransactionCommit: (cb: () => void) => void,
  queryComplete: true | ErroredQuery | Promise<true>,
  updateTTL: (ttl: TTL) => void,
): TypedView<HumanReadable<TReturn>> {
  const v = new ArrayView<HumanReadable<TReturn>>(
    input,
    format,
    queryComplete,
    updateTTL,
  );
  v.onDestroy = onDestroy;
  onTransactionCommit(() => {
    v.flush();
  });
  return v;
}

function isCompoundKey(field: readonly string[]): field is CompoundKey {
  return Array.isArray(field) && field.length >= 1;
}

function withContext<
  TSchema extends Schema,
  TTable extends string,
  TReturn,
  TContext,
>(
  query: Query<TSchema, TTable, TReturn, TContext>,
  ctx: TContext,
): QueryInternals<TSchema, TTable, TReturn, TContext> {
  assert('withContext' in query);
  return (query as any).withContext(ctx);
}
