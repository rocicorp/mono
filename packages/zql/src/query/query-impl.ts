/* eslint-disable @typescript-eslint/naming-convention */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {resolver} from '@rocicorp/resolver';
import {assert} from '../../../shared/src/asserts.js';
import {hashOfAST} from '../../../zero-protocol/src/ast-hash.js';
import type {AST, Condition, Ordering} from '../../../zero-protocol/src/ast.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import {
  normalizeTableSchema,
  type NormalizedTableSchema,
} from '../../../zero-schema/src/normalize-table-schema.js';
import {
  isFieldRelationship,
  isJunctionRelationship,
  type PullSchemaForRelationship,
  type TableSchema,
  type TableSchemaToRow,
} from '../../../zero-schema/src/table-schema.js';
import {buildPipeline, type BuilderDelegate} from '../builder/builder.js';
import {ArrayView} from '../ivm/array-view.js';
import type {Input} from '../ivm/operator.js';
import type {Format, ViewFactory} from '../ivm/view.js';
import {dnf} from './dnf.js';
import {
  and,
  cmp,
  ExpressionBuilder,
  type ExpressionFactory,
} from './expression.js';
import type {AdvancedQuery} from './query-internal.js';
import type {
  AddSubselect,
  DefaultQueryResultRow,
  GetFieldTypeNoUndefined,
  MakeSingular,
  Operator,
  Parameter,
  Query,
  QueryType,
  Smash,
} from './query.js';
import type {TypedView} from './typed-view.js';

export function newQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType = DefaultQueryResultRow<TSchema>,
>(delegate: QueryDelegate, tableSchema: TSchema): Query<TSchema, TReturn> {
  return new QueryImpl(delegate, normalizeTableSchema(tableSchema));
}

function newQueryWithDetails<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(
  delegate: QueryDelegate,
  schema: NormalizedTableSchema,
  ast: AST,
  format: Format | undefined,
): Query<TSchema, TReturn> {
  return new QueryImpl(delegate, schema, ast, format);
}

export type CommitListener = () => void;
export type GotCallback = (got: boolean) => void;
export interface QueryDelegate extends BuilderDelegate {
  addServerQuery(ast: AST, gotCallback?: GotCallback | undefined): () => void;
  onTransactionCommit(cb: CommitListener): () => void;
  batchViewUpdates<T>(applyViewUpdates: () => T): T;
}

export function staticParam<TAnchor, TField extends keyof TAnchor>(
  anchorClass: 'authData' | 'preMutationRow',
  field: TField,
): Parameter<TAnchor, TField, TAnchor[TField]> {
  return {
    type: 'static',
    anchor: anchorClass,
    field,
  };
}

let subqueryFilterCount = 0;

export const SUBQ_PREFIX = 'zsubq_';

export abstract class AbstractQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType = DefaultQueryResultRow<TSchema>,
> implements AdvancedQuery<TSchema, TReturn>
{
  readonly #ast: AST;
  readonly #schema: NormalizedTableSchema;
  readonly #format: Format;
  #hash: string = '';

  constructor(
    schema: NormalizedTableSchema,
    ast: AST,
    format?: Format | undefined,
  ) {
    this.#ast = ast;
    this.#format = format ?? {singular: false, relationships: {}};
    this.#schema = schema;
  }

  get format(): Format {
    return this.#format;
  }

  hash(): string {
    if (!this.#hash) {
      const ast = this._completeAst();
      const hash = hashOfAST(ast);
      this.#hash = hash;
    }
    return this.#hash;
  }

  protected abstract _newQuery<
    TSchema extends TableSchema,
    TReturn extends QueryType,
  >(
    schema: NormalizedTableSchema,
    ast: AST,
    format: Format | undefined,
  ): Query<TSchema, TReturn>;

  one(): Query<TSchema, MakeSingular<TReturn>> {
    return this._newQuery(
      this.#schema,
      {
        ...this.#ast,
        limit: 1,
      },
      {
        ...this.#format,
        singular: true,
      },
    );
  }

  whereExists(
    relationship: keyof TSchema['relationships'],
  ): Query<TSchema, TReturn>;
  whereExists<TRelationship extends keyof TSchema['relationships'] & string>(
    relationship: TRelationship,
    cb: (
      query: Query<
        PullSchemaForRelationship<TSchema, TRelationship>,
        DefaultQueryResultRow<PullSchemaForRelationship<TSchema, TRelationship>>
      >,
    ) => Query<TableSchema, QueryType> = q => q as any,
  ): Query<TSchema, TReturn> {
    return this.where(({exists}) => exists(relationship, cb));
  }

  related<TRelationship extends keyof TSchema['relationships']>(
    relationship: TRelationship,
  ): Query<
    TSchema,
    AddSubselect<
      Query<
        PullSchemaForRelationship<TSchema, TRelationship>,
        DefaultQueryResultRow<PullSchemaForRelationship<TSchema, TRelationship>>
      >,
      TReturn,
      TRelationship & string
    >
  >;
  related<
    TRelationship extends keyof TSchema['relationships'] & string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    TSub extends Query<any, any>,
  >(
    relationship: TRelationship,
    cb: (
      query: Query<
        PullSchemaForRelationship<TSchema, TRelationship>,
        DefaultQueryResultRow<PullSchemaForRelationship<TSchema, TRelationship>>
      >,
    ) => TSub = q => q as any,
  ) {
    if (relationship.startsWith(SUBQ_PREFIX)) {
      throw new Error(
        `Relationship names may not start with "${SUBQ_PREFIX}". That is a reserved prefix.`,
      );
    }
    const related = this.#schema.relationships[relationship as string];
    assert(related, 'Invalid relationship');
    const fieldRelationship = related;
    const junctionRelationship = related;
    if (isFieldRelationship(fieldRelationship)) {
      const destSchema = fieldRelationship.dest.schema;
      const sq = cb(
        this._newQuery(
          destSchema,
          {
            table: destSchema.tableName,
            alias: relationship as string,
          },
          undefined,
        ),
      ) as unknown as QueryImpl<any, any>;
      return this._newQuery(
        this.#schema,
        {
          ...this.#ast,
          related: [
            ...(this.#ast.related ?? []),
            {
              correlation: {
                parentField: fieldRelationship.source,
                childField: fieldRelationship.dest.field,
                op: '=',
              },
              subquery: addPrimaryKeysToAst(destSchema, sq.#ast),
            },
          ],
        },
        {
          ...this.#format,
          relationships: {
            ...this.#format.relationships,
            [relationship as string]: sq.#format,
          },
        },
      );
    }

    if (isJunctionRelationship(junctionRelationship)) {
      const destSchema = junctionRelationship.dest.schema;
      const junctionSchema = junctionRelationship.junction.schema;
      const sq = cb(
        this._newQuery(
          destSchema,
          {
            table: destSchema.tableName,
            alias: relationship as string,
          },
          undefined,
        ),
      ) as unknown as QueryImpl<any, any>;
      return this._newQuery(
        this.#schema,
        {
          ...this.#ast,
          related: [
            ...(this.#ast.related ?? []),
            {
              correlation: {
                parentField: junctionRelationship.source,
                childField: junctionRelationship.junction.sourceField,
                op: '=',
              },
              subquery: {
                table: junctionSchema.tableName,
                alias: relationship as string,
                orderBy: addPrimaryKeys(junctionSchema, undefined),
                related: [
                  {
                    correlation: {
                      parentField: junctionRelationship.junction.destField,
                      childField: junctionRelationship.dest.field,
                      op: '=',
                    },
                    hidden: true,
                    subquery: addPrimaryKeysToAst(destSchema, sq.#ast),
                  },
                ],
              },
            },
          ],
        },
        {
          ...this.#format,
          relationships: {
            ...this.#format.relationships,
            [relationship as string]: sq.#format,
          },
        },
      );
    }
    throw new Error(`Invalid relationship ${relationship as string}`);
  }

  where(
    fieldOrExpressionFactory: string | ExpressionFactory<TSchema>,
    opOrValue?:
      | Operator
      | GetFieldTypeNoUndefined<any, any, any>
      | Parameter<any, any, any>,
    value?: GetFieldTypeNoUndefined<any, any, any> | Parameter<any, any, any>,
  ): Query<TSchema, TReturn> {
    let cond: Condition;

    if (typeof fieldOrExpressionFactory === 'function') {
      cond = fieldOrExpressionFactory(new ExpressionBuilder(this.#exists));
    } else {
      assert(opOrValue !== undefined, 'Invalid condition');
      cond = cmp(fieldOrExpressionFactory, opOrValue, value);
    }

    const existingWhere = this.#ast.where;
    if (existingWhere) {
      cond = and(existingWhere, cond);
    }

    return this._newQuery(
      this.#schema,
      {
        ...this.#ast,
        where: dnf(cond),
      },
      this.#format,
    );
  }

  start(
    row: Partial<TableSchemaToRow<TSchema>>,
    opts?: {inclusive: boolean} | undefined,
  ): Query<TSchema, TReturn> {
    return this._newQuery(
      this.#schema,
      {
        ...this.#ast,
        start: {
          row,
          exclusive: !opts?.inclusive,
        },
      },
      this.#format,
    );
  }

  limit(limit: number): Query<TSchema, TReturn> {
    if (limit < 0) {
      throw new Error('Limit must be non-negative');
    }
    if ((limit | 0) !== limit) {
      throw new Error('Limit must be an integer');
    }

    return this._newQuery(
      this.#schema,
      {
        ...this.#ast,
        limit,
      },
      this.#format,
    );
  }

  orderBy<TSelector extends keyof TSchema['columns']>(
    field: TSelector,
    direction: 'asc' | 'desc',
  ): Query<TSchema, TReturn> {
    return this._newQuery(
      this.#schema,
      {
        ...this.#ast,
        orderBy: [...(this.#ast.orderBy ?? []), [field as string, direction]],
      },
      this.#format,
    );
  }

  #exists = (
    relationship: string,
    cb: (
      query: Query<TableSchema, QueryType>,
    ) => Query<TableSchema, QueryType> = q => q,
  ): Condition => {
    ++subqueryFilterCount;

    const related = this.#schema.relationships[relationship];
    assert(related, 'Invalid relationship');
    const fieldRelationship = related;
    const junctionRelationship = related;

    if (isFieldRelationship(fieldRelationship)) {
      const destSchema = fieldRelationship.dest.schema;
      const sq = cb(
        this._newQuery(
          destSchema,
          {
            table: destSchema.tableName,
            alias: `${SUBQ_PREFIX}${subqueryFilterCount}_${relationship}`,
          },
          undefined,
        ),
      ) as unknown as QueryImpl<any, any>;
      return {
        type: 'correlatedSubquery',
        related: {
          correlation: {
            parentField: fieldRelationship.source,
            childField: fieldRelationship.dest.field,
            op: '=',
          },
          subquery: addPrimaryKeysToAst(destSchema, sq.#ast),
        },
        op: 'EXISTS',
      };
    }

    if (isJunctionRelationship(junctionRelationship)) {
      const destSchema = junctionRelationship.dest.schema;
      const junctionSchema = junctionRelationship.junction.schema;
      const queryToDest = cb(
        this._newQuery(
          destSchema,
          {
            table: destSchema.tableName,
            alias: `${SUBQ_PREFIX}${subqueryFilterCount}_${relationship}`,
          },
          undefined,
        ),
      ) as unknown as QueryImpl<any, any>;

      return {
        type: 'correlatedSubquery',
        related: {
          correlation: {
            parentField: junctionRelationship.source,
            childField: junctionRelationship.junction.sourceField,
            op: '=',
          },
          subquery: {
            table: junctionSchema.tableName,
            alias: `${SUBQ_PREFIX}${subqueryFilterCount}_${relationship}`,
            orderBy: addPrimaryKeys(junctionSchema, undefined),
            where: {
              type: 'correlatedSubquery',
              related: {
                correlation: {
                  parentField: junctionRelationship.junction.destField,
                  childField: junctionRelationship.dest.field,
                  op: '=',
                },
                subquery: addPrimaryKeysToAst(destSchema, queryToDest.#ast),
              },
              op: 'EXISTS',
            },
          },
        },
        op: 'EXISTS',
      };
    }

    throw new Error(`Invalid relationship ${relationship as string}`);
  };

  #completedAST: AST | undefined;

  protected _completeAst(): AST {
    if (!this.#completedAST) {
      const finalOrderBy = addPrimaryKeys(this.#schema, this.#ast.orderBy);
      if (this.#ast.start) {
        const {row} = this.#ast.start;
        const narrowedRow: Row = {};
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
          orderBy: addPrimaryKeys(this.#schema, this.#ast.orderBy),
        };
      }
    }
    return this.#completedAST;
  }

  abstract materialize(): TypedView<Smash<TReturn>>;
  abstract materialize<T>(factory: ViewFactory<TSchema, TReturn, T>): T;
  abstract run(): Smash<TReturn>;
  abstract preload(): {
    cleanup: () => void;
    complete: Promise<void>;
  };
}

export const astForTestingSymbol = Symbol();

export class QueryImpl<
  TSchema extends TableSchema,
  TReturn extends QueryType = DefaultQueryResultRow<TSchema>,
> extends AbstractQuery<TSchema, TReturn> {
  readonly #delegate: QueryDelegate;
  readonly #ast: AST;

  constructor(
    delegate: QueryDelegate,
    schema: NormalizedTableSchema,
    ast: AST = {table: schema.tableName},
    format?: Format | undefined,
  ) {
    super(schema, ast, format);
    this.#delegate = delegate;
    this.#ast = ast;
  }

  // Not part of Query or QueryInternal interface
  get [astForTestingSymbol](): AST {
    return this.#ast;
  }

  protected _newQuery<TSchema extends TableSchema, TReturn extends QueryType>(
    schema: NormalizedTableSchema,
    ast: AST,
    format: Format | undefined,
  ): Query<TSchema, TReturn> {
    return newQueryWithDetails(this.#delegate, schema, ast, format);
  }

  materialize<T>(factory?: ViewFactory<TSchema, TReturn, T>): T {
    const ast = this._completeAst();
    const removeServerQuery = this.#delegate.addServerQuery(ast);

    const input = buildPipeline(ast, this.#delegate, undefined);
    let removeCommitObserver: (() => void) | undefined;

    const onDestroy = () => {
      input.destroy();
      removeCommitObserver?.();
      removeServerQuery();
    };

    const view = this.#delegate.batchViewUpdates(() =>
      (factory ?? arrayViewFactory)(this, input, this.format, onDestroy, cb => {
        removeCommitObserver = this.#delegate.onTransactionCommit(cb);
      }),
    );

    return view as T;
  }

  run() {
    const v: TypedView<Smash<TReturn>> = this.materialize();
    const ret = v.data;
    v.destroy();
    return ret;
  }

  preload(): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    const {resolve, promise: complete} = resolver<void>();
    const ast = this._completeAst();
    const unsub = this.#delegate.addServerQuery(ast, got => {
      if (got) {
        resolve();
      }
    });
    return {
      cleanup: unsub,
      complete,
    };
  }
}

function addPrimaryKeys(
  schema: NormalizedTableSchema,
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

function addPrimaryKeysToAst(schema: NormalizedTableSchema, ast: AST): AST {
  return {
    ...ast,
    orderBy: addPrimaryKeys(schema, ast.orderBy),
  };
}

function arrayViewFactory<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(
  _query: Query<TSchema, TReturn>,
  input: Input,
  format: Format,
  onDestroy: () => void,
  onTransactionCommit: (cb: () => void) => void,
): TypedView<Smash<TReturn>> {
  const v = new ArrayView<Smash<TReturn>>(input, format);
  v.onDestroy = onDestroy;
  onTransactionCommit(() => {
    v.flush();
  });
  return v;
}
