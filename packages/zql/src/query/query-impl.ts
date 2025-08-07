/* eslint-disable @typescript-eslint/naming-convention */
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
import {
  type GetFilterType,
  type HumanReadable,
  type PreloadOptions,
  type PullRow,
  type Query,
  type RunOptions,
} from './query.ts';
import {DEFAULT_PRELOAD_TTL_MS, DEFAULT_TTL_MS, type TTL} from './ttl.ts';
import type {TypedView} from './typed-view.ts';

export type AnyQuery = Query<Schema, string, any>;

const astSymbol = Symbol();

export function ast(query: Query<Schema, string, any>): AST {
  return (query as AbstractQuery<Schema, string>)[astSymbol];
}

export function newQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
>(
  delegate: QueryDelegate | undefined,
  schema: TSchema,
  table: TTable,
): Query<TSchema, TTable> {
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

export const newQuerySymbol = Symbol();

export abstract class AbstractQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> implements Query<TSchema, TTable, TReturn>
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
    currentJunction?: string | undefined,
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

  delegate(delegate: QueryDelegate): Query<TSchema, TTable, TReturn> {
    return this[newQuerySymbol](
      delegate,
      this.#schema,
      this.#tableName,
      this._ast,
      this.format,
      this.customQueryID,
      this.#currentJunction,
    );
  }

  nameAndArgs(
    name: string,
    args: ReadonlyArray<ReadonlyJSONValue>,
  ): Query<TSchema, TTable, TReturn> {
    return this[newQuerySymbol](
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
    return this._completeAst();
  }

  hash(): string {
    if (!this.#hash) {
      this.#hash = hashOfAST(this._completeAst());
    }
    return this.#hash;
  }

  // TODO(arv): Put this in the delegate?
  protected abstract [newQuerySymbol]<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    delegate: QueryDelegate | undefined,
    schema: TSchema,
    table: TTable,
    ast: AST,
    format: Format,
    customQueryID: CustomQueryID | undefined,
    currentJunction: string | undefined,
  ): AbstractQuery<TSchema, TTable, TReturn>;

  one = (): Query<TSchema, TTable, TReturn | undefined> =>
    this[newQuerySymbol](
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
    cb?: (q: AnyQuery) => AnyQuery,
  ): Query<TSchema, TTable, TReturn> =>
    this.where(({exists}) => exists(relationship, cb));

  related = (
    relationship: string,
    cb?: (q: AnyQuery) => AnyQuery,
  ): AnyQuery => {
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
      let q: AnyQuery = this[newQuerySymbol](
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
      );
      if (cardinality === 'one') {
        q = q.one();
      }
      const sq = cb(q) as AbstractQuery<Schema, string>;
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

      return this[newQuerySymbol](
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
      );
    }

    if (isTwoHop(related)) {
      const [firstRelation, secondRelation] = related;
      const {destSchema} = secondRelation;
      const junctionSchema = firstRelation.destSchema;
      const sq = cb(
        this[newQuerySymbol](
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
        ),
      ) as unknown as QueryImpl<Schema, string>;

      assert(isCompoundKey(firstRelation.sourceField), 'Invalid relationship');
      assert(isCompoundKey(firstRelation.destField), 'Invalid relationship');
      assert(isCompoundKey(secondRelation.sourceField), 'Invalid relationship');
      assert(isCompoundKey(secondRelation.destField), 'Invalid relationship');

      return this[newQuerySymbol](
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
                      sq._ast,
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
      );
    }

    throw new Error(`Invalid relationship ${relationship}`);
  };

  where = (
    fieldOrExpressionFactory: string | ExpressionFactory<TSchema, TTable>,
    opOrValue?: SimpleOperator | GetFilterType<any, any, any> | Parameter,
    value?: GetFilterType<any, any, any> | Parameter,
  ): Query<TSchema, TTable, TReturn> => {
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

    return this[newQuerySymbol](
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
    opts?: {inclusive: boolean} | undefined,
  ): Query<TSchema, TTable, TReturn> =>
    this[newQuerySymbol](
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

  limit = (limit: number): Query<TSchema, TTable, TReturn> => {
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

    return this[newQuerySymbol](
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
  ): Query<TSchema, TTable, TReturn> => {
    if (this.#currentJunction) {
      throw new NotImplementedError(
        'Order by is not supported in junction relationships yet. Junction relationship being ordered: ' +
          this.#currentJunction,
      );
    }
    return this[newQuerySymbol](
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
    cb: (query: AnyQuery) => AnyQuery = q => q,
  ): Condition => {
    const related = this.#schema.relationships[this.#tableName][relationship];
    assert(related, 'Invalid relationship');

    if (isOneHop(related)) {
      const {destSchema, sourceField, destField} = related[0];
      assert(isCompoundKey(sourceField), 'Invalid relationship');
      assert(isCompoundKey(destField), 'Invalid relationship');

      const sq = cb(
        this[newQuerySymbol](
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
        ),
      ) as unknown as QueryImpl<any, any>;
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
        this[newQuerySymbol](
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
                  (queryToDest as QueryImpl<any, any>)._ast,
                ),
              },
              op: 'EXISTS',
            },
          },
        },
        op: 'EXISTS',
      };
    }

    throw new Error(`Invalid relationship ${relationship}`);
  };

  #completedAST: AST | undefined;

  protected _completeAst(): AST {
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

  then<TResult1 = HumanReadable<TReturn>, TResult2 = never>(
    onFulfilled?:
      | ((value: HumanReadable<TReturn>) => TResult1 | PromiseLike<TResult1>)
      | undefined
      | null,
    onRejected?:
      | ((reason: any) => TResult2 | PromiseLike<TResult2>)
      | undefined
      | null,
  ): PromiseLike<TResult1 | TResult2> {
    return this.run().then(onFulfilled, onRejected);
  }

  abstract materialize(
    ttl?: TTL | undefined,
  ): TypedView<HumanReadable<TReturn>>;
  abstract materialize<T>(
    factory: ViewFactory<TSchema, TTable, TReturn, T>,
    ttl?: TTL | undefined,
  ): T;

  abstract run(options?: RunOptions): Promise<HumanReadable<TReturn>>;

  abstract preload(): {
    cleanup: () => void;
    complete: Promise<void>;
  };
}

const completedAstSymbol = Symbol();

export function completedAST(q: Query<Schema, string, any>) {
  return (q as QueryImpl<Schema, string>)[completedAstSymbol];
}

export class QueryImpl<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> extends AbstractQuery<TSchema, TTable, TReturn> {
  readonly #system: System;

  constructor(
    delegate: QueryDelegate | undefined,
    schema: TSchema,
    tableName: TTable,
    ast: AST = {table: tableName},
    format: Format = defaultFormat,
    system: System = 'client',
    customQueryID?: CustomQueryID | undefined,
    currentJunction?: string | undefined,
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

  get [completedAstSymbol](): AST {
    return this._completeAst();
  }

  protected [newQuerySymbol]<
    TSchema extends Schema,
    TTable extends string,
    TReturn,
  >(
    delegate: QueryDelegate | undefined,
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
    customQueryID: CustomQueryID | undefined,
    currentJunction: string | undefined,
  ): QueryImpl<TSchema, TTable, TReturn> {
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
    factoryOrTTL?: ViewFactory<TSchema, TTable, TReturn, T> | TTL,
    ttl: TTL = DEFAULT_TTL_MS,
  ): T {
    const delegate = must(
      this._delegate,
      'materialize requires a query delegate to be set',
    );
    let factory: ViewFactory<TSchema, TTable, TReturn, T> | undefined;
    if (typeof factoryOrTTL === 'function') {
      factory = factoryOrTTL;
    } else {
      ttl = factoryOrTTL ?? DEFAULT_TTL_MS;
    }
    const ast = this._completeAst();
    const queryID = this.customQueryID
      ? hashOfNameAndArgs(this.customQueryID.name, this.customQueryID.args)
      : this.hash();
    const queryCompleteResolver = resolver<true>();
    let queryComplete = delegate.defaultQueryComplete;
    const updateTTL = (newTTL: TTL) => {
      this.customQueryID
        ? delegate.updateCustomQuery(this.customQueryID, newTTL)
        : delegate.updateServerQuery(ast, newTTL);
    };

    const gotCallback: GotCallback = got => {
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

    const removeAddedQuery = this.customQueryID
      ? delegate.addCustomQuery(this.customQueryID, ttl, gotCallback)
      : delegate.addServerQuery(ast, ttl, gotCallback);

    const input = buildPipeline(ast, delegate);

    const view = delegate.batchViewUpdates(() =>
      (factory ?? arrayViewFactory)(
        this,
        input,
        this.format,
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

  run(options?: RunOptions): Promise<HumanReadable<TReturn>> {
    const delegate = must(
      this._delegate,
      'run requires a query delegate to be set',
    );
    delegate.assertValidRunOptions(options);
    const v: TypedView<HumanReadable<TReturn>> = this.materialize(options?.ttl);
    if (options?.type === 'complete') {
      return new Promise(resolve => {
        v.addListener((data, type) => {
          if (type === 'complete') {
            v.destroy();
            resolve(data as HumanReadable<TReturn>);
          }
        });
      });
    }

    options?.type satisfies 'unknown' | undefined;

    const ret = v.data;
    v.destroy();
    return Promise.resolve(ret);
  }

  preload(options?: PreloadOptions): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    const delegate = must(
      this._delegate,
      'preload requires a query delegate to be set',
    );
    const ttl = options?.ttl ?? DEFAULT_PRELOAD_TTL_MS;
    const {resolve, promise: complete} = resolver<void>();
    if (this.customQueryID) {
      const cleanup = delegate.addCustomQuery(this.customQueryID, ttl, got => {
        if (got) {
          resolve();
        }
      });
      return {
        cleanup,
        complete,
      };
    }

    const ast = this._completeAst();
    const cleanup = delegate.addServerQuery(ast, ttl, got => {
      if (got) {
        resolve();
      }
    });
    return {
      cleanup,
      complete,
    };
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
>(
  _query: AbstractQuery<TSchema, TTable, TReturn>,
  input: Input,
  format: Format,
  onDestroy: () => void,
  onTransactionCommit: (cb: () => void) => void,
  queryComplete: true | Promise<true>,
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
