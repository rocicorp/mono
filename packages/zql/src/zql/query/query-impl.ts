/* eslint-disable @typescript-eslint/naming-convention */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {resolver} from '@rocicorp/resolver';
import {assert} from '../../../../shared/src/asserts.js';
import {hashOfAST} from '../../../../zero-protocol/src/ast-hash.js';
import type {
  AST,
  Condition,
  Ordering,
} from '../../../../zero-protocol/src/ast.js';
import type {Row} from '../../../../zero-protocol/src/data.js';
import {buildPipeline, type BuilderDelegate} from '../builder/builder.js';
import {ArrayView} from '../ivm/array-view.js';
import type {Input} from '../ivm/operator.js';
import type {Format, ViewFactory} from '../ivm/view.js';
import {
  normalizeTableSchema,
  type NormalizedTableSchema,
} from './normalize-table-schema.js';
import type {QueryInternal} from './query-internal.js';
import type {
  AddSelections,
  AddSubselect,
  DefaultQueryResultRow,
  GetFieldTypeNoNullOrUndefined,
  MakeSingular,
  Operator,
  Parameter,
  Query,
  QueryType,
  SchemaToRow,
  Selector,
  Smash,
} from './query.js';
import {
  isFieldRelationship,
  isJunctionRelationship,
  type PullSchemaForRelationship,
  type TableSchema,
} from './schema.js';
import type {TypedView} from './typed-view.js';
import {and, cmp, type GenericCondition} from './expression.js';

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

export abstract class AbstractQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType = DefaultQueryResultRow<TSchema>,
> implements QueryInternal<TSchema, TReturn>
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

  select<TFields extends Selector<TSchema>[]>(
    ..._fields: TFields
  ): Query<TSchema, AddSelections<TSchema, TFields, TReturn>> {
    // we return all columns for now so we ignore the selection set and only use it for type inference
    return this._newQuery(this.#schema, this.#ast, this.#format);
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
    TRelationship extends keyof TSchema['relationships'],
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
    const related = this.#schema.relationships[relationship as string];
    assert(related, 'Invalid relationship');
    const related1 = related;
    const related2 = related;
    if (isFieldRelationship(related1)) {
      const destSchema = related1.dest.schema;
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
                parentField: related1.source,
                childField: related1.dest.field,
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

    if (isJunctionRelationship(related2)) {
      const destSchema = related2.dest.schema;
      const junctionSchema = related2.junction.schema;
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
                parentField: related2.source,
                childField: related2.junction.sourceField,
                op: '=',
              },
              subquery: {
                table: junctionSchema.tableName,
                alias: relationship as string,
                orderBy: addPrimaryKeys(junctionSchema, undefined),
                related: [
                  {
                    correlation: {
                      parentField: related2.junction.destField,
                      childField: related2.dest.field,
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
    field: string | GenericCondition<TSchema>,
    opOrValue?:
      | Operator
      | GetFieldTypeNoNullOrUndefined<any, any, any>
      | Parameter<any, any, any>,
    value?:
      | GetFieldTypeNoNullOrUndefined<any, any, any>
      | Parameter<any, any, any>,
  ): Query<TSchema, TReturn> {
    let cond: Condition;
    if (opOrValue === undefined && value === undefined) {
      assert(typeof field !== 'string', `Invalid condition: ${field}`);
      cond = field as Condition;
    } else {
      cond = cmp(field as string, opOrValue!, value) as Condition;
    }

    const existingWhere = this.#ast.where;
    if (existingWhere) {
      cond = and(existingWhere, cond);
    }

    return this._newQuery(
      this.#schema,
      {
        ...this.#ast,
        where: cond,
      },
      this.#format,
    );
  }

  start(
    row: Partial<SchemaToRow<TSchema>>,
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
