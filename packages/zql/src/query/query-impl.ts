/* eslint-disable @typescript-eslint/naming-convention */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {resolver} from '@rocicorp/resolver';
import {assert} from '../../../shared/src/asserts.ts';
import type {Writable} from '../../../shared/src/writable.ts';
import {hashOfAST} from '../../../zero-protocol/src/ast-hash.ts';
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
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  isOneHop,
  isTwoHop,
  type TableSchema,
} from '../../../zero-schema/src/table-schema.ts';
import {buildPipeline, type BuilderDelegate} from '../builder/builder.ts';
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
import {
  type GetFilterType,
  type HumanReadable,
  type PreloadOptions,
  type PullRow,
  type Query,
  type RunOptions,
} from './query.ts';
import {DEFAULT_TTL, type TTL} from './ttl.ts';
import type {TypedView} from './typed-view.ts';
import {NotImplementedError} from '../error.ts';

type AnyQuery = Query<Schema, string, any>;

const astSymbol = Symbol();

export function ast(query: Query<Schema, string, any>): AST {
  return (query as AbstractQuery<Schema, string>)[astSymbol];
}

export function newQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
>(
  delegate: QueryDelegate,
  schema: TSchema,
  table: TTable,
): Query<TSchema, TTable> {
  return new QueryImpl(delegate, schema, table, {table}, defaultFormat);
}

export type CommitListener = () => void;

export type GotCallback = (got: boolean) => void;

export interface NewQueryDelegate {
  newQuery<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    schema: TSchema,
    table: TTable,
    ast: AST,
    format: Format,
  ): Query<TSchema, TTable, TReturn>;
}

export interface QueryDelegate extends BuilderDelegate {
  addServerQuery(
    ast: AST,
    ttl: TTL,
    gotCallback?: GotCallback | undefined,
  ): () => void;
  updateServerQuery(ast: AST, ttl: TTL): void;
  onTransactionCommit(cb: CommitListener): () => void;
  batchViewUpdates<T>(applyViewUpdates: () => T): T;
  onQueryMaterialized(hash: string, ast: AST, duration: number): void;

  /**
   * Asserts that the `RunOptions` provided to the `run` method are supported in
   * this context. For example, in a custom mutator, the `{type: 'complete'}`
   * option is not supported and this will throw.
   */
  assertValidRunOptions(options?: RunOptions): void;

  /**
   * Client queries start off as false (`unknown`) and are set to true when the
   * server sends the gotQueries message.
   *
   * For things like ZQLite the default is true (aka `complete`) because the
   * data is always available.
   */
  readonly defaultQueryComplete: boolean;
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
  readonly #tableName: TTable;
  readonly #ast: AST;
  readonly format: Format;
  #hash: string = '';
  readonly #system: System;
  readonly #currentJunction: string | undefined;

  constructor(
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
    system: System,
    currentJunction?: string | undefined,
  ) {
    this.#schema = schema;
    this.#tableName = tableName;
    this.#ast = ast;
    this.format = format;
    this.#system = system;
    this.#currentJunction = currentJunction;
  }

  get [astSymbol](): AST {
    return this.#ast;
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
    schema: TSchema,
    table: TTable,
    ast: AST,
    format: Format,
    currentJunction: string | undefined,
  ): AbstractQuery<TSchema, TTable, TReturn>;

  one = (): Query<TSchema, TTable, TReturn | undefined> =>
    this[newQuerySymbol](
      this.#schema,
      this.#tableName,
      {
        ...this.#ast,
        limit: 1,
      },
      {
        ...this.format,
        singular: true,
      },
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
        this.#schema,
        this.#tableName,
        {
          ...this.#ast,
          related: [
            ...(this.#ast.related ?? []),
            {
              system: this.#system,
              correlation: {
                parentField: sourceField,
                childField: destField,
              },
              subquery: addPrimaryKeysToAst(
                this.#schema.tables[destSchema],
                sq.#ast,
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
        this.#currentJunction,
      );
    }

    if (isTwoHop(related)) {
      const [firstRelation, secondRelation] = related;
      const {destSchema} = secondRelation;
      const junctionSchema = firstRelation.destSchema;
      const sq = cb(
        this[newQuerySymbol](
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
          relationship,
        ),
      ) as unknown as QueryImpl<Schema, string>;

      assert(isCompoundKey(firstRelation.sourceField), 'Invalid relationship');
      assert(isCompoundKey(firstRelation.destField), 'Invalid relationship');
      assert(isCompoundKey(secondRelation.sourceField), 'Invalid relationship');
      assert(isCompoundKey(secondRelation.destField), 'Invalid relationship');

      return this[newQuerySymbol](
        this.#schema,
        this.#tableName,
        {
          ...this.#ast,
          related: [
            ...(this.#ast.related ?? []),
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
                      sq.#ast,
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

    const existingWhere = this.#ast.where;
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
      this.#schema,
      this.#tableName,
      {
        ...this.#ast,
        where,
      },
      this.format,
      this.#currentJunction,
    );
  };

  start = (
    row: Partial<PullRow<TTable, TSchema>>,
    opts?: {inclusive: boolean} | undefined,
  ): Query<TSchema, TTable, TReturn> =>
    this[newQuerySymbol](
      this.#schema,
      this.#tableName,
      {
        ...this.#ast,
        start: {
          row,
          exclusive: !opts?.inclusive,
        },
      },
      this.format,
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
      this.#schema,
      this.#tableName,
      {
        ...this.#ast,
        limit,
      },
      this.format,
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
      this.#schema,
      this.#tableName,
      {
        ...this.#ast,
        orderBy: [...(this.#ast.orderBy ?? []), [field as string, direction]],
      },
      this.format,
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
          this.#schema,
          destSchema,
          {
            table: destSchema,
            alias: `${SUBQ_PREFIX}${relationship}`,
          },
          defaultFormat,
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
            sq.#ast,
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
          this.#schema,
          destSchema,
          {
            table: destSchema,
            alias: `${SUBQ_PREFIX}${relationship}`,
          },
          defaultFormat,
          relationship,
        ),
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
                  (queryToDest as QueryImpl<any, any>).#ast,
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
        this.#ast.orderBy,
      );
      if (this.#ast.start) {
        const {row} = this.#ast.start;
        const narrowedRow: Writable<IVMRow> = {};
        for (const [field] of finalOrderBy) {
          narrowedRow[field] = row[field];
        }
        this.#completedAST = {
          ...this.#ast,
          start: {
            ...this.#ast.start,
            row: narrowedRow,
          },
          orderBy: finalOrderBy,
        };
      } else {
        this.#completedAST = {
          ...this.#ast,
          orderBy: addPrimaryKeys(
            this.#schema.tables[this.#tableName],
            this.#ast.orderBy,
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

  abstract materialize(): TypedView<HumanReadable<TReturn>>;
  abstract materialize<T>(factory: ViewFactory<TSchema, TTable, TReturn, T>): T;

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
  readonly #delegate: QueryDelegate;
  readonly #system: System;

  constructor(
    delegate: QueryDelegate,
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
    system: System = 'client',
    currentJunction?: string | undefined,
  ) {
    super(schema, tableName, ast, format, system, currentJunction);
    this.#system = system;
    this.#delegate = delegate;
  }

  get [completedAstSymbol](): AST {
    return this._completeAst();
  }

  protected [newQuerySymbol]<
    TSchema extends Schema,
    TTable extends string,
    TReturn,
  >(
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
    currentJunction: string | undefined,
  ): QueryImpl<TSchema, TTable, TReturn> {
    return new QueryImpl(
      this.#delegate,
      schema,
      tableName,
      ast,
      format,
      this.#system,
      currentJunction,
    );
  }

  materialize<T>(
    factoryOrTTL?: ViewFactory<TSchema, TTable, TReturn, T> | TTL,
    ttl: TTL = DEFAULT_TTL,
  ): T {
    const t0 = Date.now();
    let factory: ViewFactory<TSchema, TTable, TReturn, T> | undefined;
    if (typeof factoryOrTTL === 'function') {
      factory = factoryOrTTL;
    } else {
      ttl = factoryOrTTL ?? DEFAULT_TTL;
    }
    const ast = this._completeAst();
    const queryCompleteResolver = resolver<true>();
    let queryComplete = this.#delegate.defaultQueryComplete;
    const removeServerQuery = this.#delegate.addServerQuery(ast, ttl, got => {
      if (got) {
        const t1 = Date.now();
        this.#delegate.onQueryMaterialized(this.hash(), ast, t1 - t0);
        queryComplete = true;
        queryCompleteResolver.resolve(true);
      }
    });

    const updateTTL = (newTTL: TTL) => {
      this.#delegate.updateServerQuery(ast, newTTL);
    };

    const input = buildPipeline(ast, this.#delegate);
    let removeCommitObserver: (() => void) | undefined;

    const onDestroy = () => {
      input.destroy();
      removeCommitObserver?.();
      removeServerQuery();
    };

    const view = this.#delegate.batchViewUpdates(() =>
      (factory ?? arrayViewFactory)(
        this,
        input,
        this.format,
        onDestroy,
        cb => {
          removeCommitObserver = this.#delegate.onTransactionCommit(cb);
        },
        queryComplete || queryCompleteResolver.promise,
        updateTTL,
      ),
    );

    return view as T;
  }

  run(options?: RunOptions): Promise<HumanReadable<TReturn>> {
    this.#delegate.assertValidRunOptions(options);
    const v: TypedView<HumanReadable<TReturn>> = this.materialize();
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
    const {resolve, promise: complete} = resolver<void>();
    const ast = this._completeAst();
    const unsub = this.#delegate.addServerQuery(
      ast,
      options?.ttl ?? DEFAULT_TTL,
      got => {
        if (got) {
          resolve();
        }
      },
    );
    return {
      cleanup: unsub,
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
