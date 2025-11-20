import {resolver} from '@rocicorp/resolver';
import {assert} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import {
  SUBQ_PREFIX,
  type AST,
  type CompoundKey,
  type Condition,
  type Parameter,
  type SimpleOperator,
  type System,
} from '../../../zero-protocol/src/ast.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import {
  hashOfAST,
  hashOfNameAndArgs,
} from '../../../zero-protocol/src/query-hash.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {buildPipeline} from '../builder/builder.ts';
import {NotImplementedError} from '../error.ts';
import {ArrayView} from '../ivm/array-view.ts';
import {defaultFormat} from '../ivm/default-format.ts';
import type {Input} from '../ivm/operator.ts';
import type {Format, ViewFactory} from '../ivm/view.ts';
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
  asQueryInternals,
  queryInternalsTag,
  type QueryInternals,
} from './query-internals.ts';
import {
  type AnyQuery,
  type AvailableRelationships,
  type ExistsOptions,
  type GetFilterType,
  type HumanReadable,
  type MaterializeOptions,
  type PreloadOptions,
  type PullRow,
  type Query,
  type QueryReturn,
  type RunOptions,
} from './query.ts';
import type {RunnableQuery} from './runnable-query.ts';
import {DEFAULT_PRELOAD_TTL_MS, DEFAULT_TTL_MS, type TTL} from './ttl.ts';
import type {TypedView} from './typed-view.ts';

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

// oxlint-disable-next-line no-explicit-any
type GetFilterTypeAny = GetFilterType<any, any, any>;

type NewQueryFunction<
  TSchema extends Schema,
  QueryReturn = AnyQuery<TSchema>,
> = <TTable extends keyof TSchema['tables'] & string>(
  this: unknown,
  tableName: TTable,
  ast: AST,
  format: Format,
  customQueryID: CustomQueryID | undefined,
  currentJunction: string | undefined,
) => QueryReturn;

export abstract class AbstractQuery<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn = PullRow<TTable, TSchema>,
  >
  implements
    Query<TSchema, TTable, TReturn>,
    QueryInternals<TSchema, TTable, TReturn>
{
  readonly [queryInternalsTag] = true;

  readonly #schema: TSchema;
  readonly #tableName: TTable;
  readonly #ast: AST;
  readonly format: Format;
  #hash: string = '';
  readonly #system: System;
  readonly #currentJunction: string | undefined;
  readonly customQueryID: CustomQueryID | undefined;
  readonly #newQuery: NewQueryFunction<TSchema, this>;

  constructor(
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
    system: System,
    customQueryID: CustomQueryID | undefined,
    currentJunction: string | undefined,
    newQuery: NewQueryFunction<TSchema>,
  ) {
    this.#schema = schema;
    this.#tableName = tableName;
    this.#ast = ast;
    this.format = format;
    this.#system = system;
    this.#currentJunction = currentJunction;
    this.customQueryID = customQueryID;
    this.#newQuery = newQuery as NewQueryFunction<TSchema, this>;
  }

  nameAndArgs(name: string, args: ReadonlyArray<ReadonlyJSONValue>): this {
    return this.#newQuery(
      this.#tableName,
      this.#ast,
      this.format,
      {
        name,
        args,
      },
      this.#currentJunction,
    );
  }

  hash(): string {
    if (!this.#hash) {
      this.#hash = hashOfAST(this.#ast);
    }
    return this.#hash;
  }

  one = (): Query<TSchema, TTable, TReturn | undefined> =>
    this.#newQuery(
      this.#tableName,
      {
        ...this.#ast,
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
  ): this => {
    const cb = typeof cbOrOptions === 'function' ? cbOrOptions : undefined;
    const opts = typeof cbOrOptions === 'function' ? options : cbOrOptions;
    const flipped = opts?.flip;
    return this.where(({exists}) =>
      exists(
        relationship,
        cb,
        flipped !== undefined ? {flip: flipped} : undefined,
      ),
    );
  };

  related = (
    relationship: string,
    cb?: (q: AnyQuery) => AnyQuery,
    // oxlint-disable-next-line no-explicit-any
  ): Query<Schema, string, any> => {
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
      const q: AnyQuery = this.#newQuery(
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
      const subQuery = asAbstractQuery(cb(q));
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

      return this.#newQuery(
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
              subquery: subQuery.#ast,
            },
          ],
        },
        {
          ...this.format,
          relationships: {
            ...this.format.relationships,
            [relationship]: subQuery.format,
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
      const sq = asAbstractQuery(
        cb(
          this.#newQuery(
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
        ),
      );

      assert(isCompoundKey(firstRelation.sourceField), 'Invalid relationship');
      assert(isCompoundKey(firstRelation.destField), 'Invalid relationship');
      assert(isCompoundKey(secondRelation.sourceField), 'Invalid relationship');
      assert(isCompoundKey(secondRelation.destField), 'Invalid relationship');

      return this.#newQuery(
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
                related: [
                  {
                    system: this.#system,
                    correlation: {
                      parentField: secondRelation.sourceField,
                      childField: secondRelation.destField,
                    },
                    subquery: sq.#ast,
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
    opOrValue?: SimpleOperator | GetFilterTypeAny | Parameter,
    value?: GetFilterTypeAny | Parameter,
  ): this => {
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

    return this.#newQuery(
      this.#tableName,
      {
        ...this.#ast,
        where,
      },
      this.format,
      this.customQueryID,
      this.#currentJunction,
    );
  };

  start = (
    row: Partial<Record<string, ReadonlyJSONValue | undefined>>,
    opts?: {inclusive: boolean},
  ): this =>
    this.#newQuery(
      this.#tableName,
      {
        ...this.#ast,
        start: {
          row,
          exclusive: !opts?.inclusive,
        },
      },
      this.format,
      this.customQueryID,
      this.#currentJunction,
    );

  limit = (limit: number): this => {
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

    return this.#newQuery(
      this.#tableName,
      {
        ...this.#ast,
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
  ): this => {
    if (this.#currentJunction) {
      throw new NotImplementedError(
        'Order by is not supported in junction relationships yet. Junction relationship being ordered: ' +
          this.#currentJunction,
      );
    }
    return this.#newQuery(
      this.#tableName,
      {
        ...this.#ast,
        orderBy: [...(this.#ast.orderBy ?? []), [field as string, direction]],
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
    const flip = options?.flip;
    const related = this.#schema.relationships[this.#tableName][relationship];
    assert(related, 'Invalid relationship');

    if (isOneHop(related)) {
      const {destSchema: destTableName, sourceField, destField} = related[0];
      assert(isCompoundKey(sourceField), 'Invalid relationship');
      assert(isCompoundKey(destField), 'Invalid relationship');

      const subQuery = asAbstractQuery(
        cb(
          this.#newQuery(
            destTableName,
            {
              table: destTableName,
              alias: `${SUBQ_PREFIX}${relationship}`,
            },
            defaultFormat,
            this.customQueryID,
            undefined,
          ),
        ),
      );
      return {
        type: 'correlatedSubquery',
        related: {
          system: this.#system,
          correlation: {
            parentField: sourceField,
            childField: destField,
          },
          subquery: subQuery.#ast,
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
        this.#newQuery(
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
            where: {
              type: 'correlatedSubquery',
              related: {
                system: this.#system,
                correlation: {
                  parentField: secondRelation.sourceField,
                  childField: secondRelation.destField,
                },
                subquery: (queryToDest as QueryImpl<Schema, string, unknown>)
                  .#ast,
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

  get ast(): AST {
    return this.#ast;
  }
}

function asAbstractQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(q: Query<TSchema, TTable, TReturn>): AbstractQuery<TSchema, TTable, TReturn> {
  assert(q instanceof AbstractQuery);
  return q;
}

export function materializeImpl<TQuery extends AnyQuery, T>(
  query: TQuery,
  delegate: QueryDelegate,
  factory: ViewFactory<
    TQuery,
    T
    // oxlint-disable-next-line no-explicit-any
  > = arrayViewFactory as any,
  options?: MaterializeOptions,
): T {
  let ttl: TTL = options?.ttl ?? DEFAULT_TTL_MS;

  const qi = asQueryInternals(query);
  const {ast, format, customQueryID} = qi;
  const queryHash = qi.hash();

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

// oxlint-disable-next-line require-await
export async function runImpl<TQuery extends AnyQuery>(
  query: TQuery,
  delegate: QueryDelegate,
  options?: RunOptions,
): Promise<HumanReadable<QueryReturn<TQuery>>> {
  delegate.assertValidRunOptions(options);
  const v: TypedView<HumanReadable<QueryReturn<TQuery>>> = materializeImpl(
    query,
    delegate,
    undefined,
    {
      ttl: options?.ttl,
    },
  );
  if (options?.type === 'complete') {
    return new Promise(resolve => {
      v.addListener((data, type) => {
        if (type === 'complete') {
          v.destroy();
          resolve(data as HumanReadable<QueryReturn<TQuery>>);
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
  return ret;
}

export function preloadImpl<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  query: Query<TSchema, TTable, TReturn>,
  delegate: QueryDelegate,
  options?: PreloadOptions,
): {
  cleanup: () => void;
  complete: Promise<void>;
} {
  const qi = asQueryInternals(query);
  const ttl = options?.ttl ?? DEFAULT_PRELOAD_TTL_MS;
  const {resolve, promise: complete} = resolver<void>();
  const {customQueryID, ast} = qi;
  if (customQueryID) {
    const cleanup = delegate.addCustomQuery(ast, customQueryID, ttl, got => {
      if (got) {
        resolve();
      }
    });
    return {
      cleanup,
      complete,
    };
  }

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

export class QueryImpl<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn = PullRow<TTable, TSchema>,
  >
  extends AbstractQuery<TSchema, TTable, TReturn>
  implements RunnableQuery<TSchema, TTable, TReturn>
{
  readonly #delegate: QueryDelegate | undefined;

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
      schema,
      tableName,
      ast,
      format,
      system,
      customQueryID,
      currentJunction,
      (tableName, ast, format, customQueryID, currentJunction) =>
        new QueryImpl(
          delegate,
          schema,
          tableName,
          ast,
          format,
          system,
          customQueryID,
          currentJunction,
        ),
    );
    this.#delegate = delegate;
  }

  declare one: () => RunnableQuery<TSchema, TTable, TReturn | undefined>;

  declare related: <
    TRelationship extends AvailableRelationships<TTable, TSchema>,
  >(
    relationship: TRelationship,
    cb?: (q: AnyQuery) => AnyQuery,
    // oxlint-disable-next-line no-explicit-any
  ) => RunnableQuery<TSchema, string, any>;

  // declare related: <
  //   TRelationship extends AvailableRelationships<TTable, TSchema>,
  // >(
  //   relationship: TRelationship,
  // ) => RunnableQuery<
  //   TSchema,
  //   TTable,
  //   RelatedQueryReturn<TReturn, TTable, TSchema, TRelationship>
  // >;
  // declare related: <
  //   TRelationship extends AvailableRelationships<TTable, TSchema>,
  //   TSub extends AnyQuery<TSchema>,
  // >(
  //   relationship: TRelationship,
  //   cb: RelatedCallback<TSchema, TTable, TRelationship, TSub>,
  // ) => TSub extends Query<TSchema, string, infer TSubReturn>
  //   ? Query<TSchema, TTable, AddSubreturn<TReturn, TSubReturn, TRelationship>>
  //   : never;

  // related<TRelationship extends AvailableRelationships<TTable, TSchema>>(
  //     relationship: TRelationship,
  //   ): RunnableQuery<
  //     TSchema,
  //     TTable,
  //     AddSubreturn<
  //       TReturn,
  //       DestRow<TSchema, TTable, TRelationship>,
  //       TRelationship
  //     >
  //   >;
  //   related<
  //     TRelationship extends AvailableRelationships<TTable, TSchema>,
  //     TSub extends RunnableQuery<TSchema, string, any>,
  //   >(
  //     relationship: TRelationship,
  //     cb: RelatedCallback<TSchema, TTable, TRelationship, TSub>,
  //   ): RunnableQuery<
  //     TSchema,
  //     TTable,
  //     AddSubreturn<TReturn, QueryReturn<TSub>, TRelationship>
  //   >;

  run(options?: RunOptions): Promise<HumanReadable<TReturn>> {
    return must(this.#delegate).run(this, options) as Promise<
      HumanReadable<TReturn>
    >;
  }
}

function arrayViewFactory<
  TSchema extends Schema,
  TTable extends string,
  TReturn,
>(
  _query: QueryInternals<TSchema, TTable, TReturn>,
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

function isOneHop<T>(r: readonly T[]): r is readonly [T] {
  return r.length === 1;
}

function isTwoHop<T>(r: readonly T[]): r is readonly [T, T] {
  return r.length === 2;
}
