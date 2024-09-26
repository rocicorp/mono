/* eslint-disable @typescript-eslint/naming-convention */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {assert} from 'shared/src/asserts.js';
import type {AST, Ordering} from '../ast/ast.js';
import {type BuilderDelegate, buildPipeline} from '../builder/builder.js';
import {ArrayView, type Format} from '../ivm/array-view.js';
import type {Row} from '../ivm/data.js';
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
  type Lazy,
  type PullSchemaForRelationship,
  type Schema,
} from './schema.js';
import type {TypedView} from './typed-view.js';

export function newQuery<
  TSchema extends Schema,
  TReturn extends QueryType = DefaultQueryResultRow<TSchema>,
>(delegate: QueryDelegate, schema: TSchema): Query<TSchema, TReturn> {
  return new QueryImpl(delegate, schema);
}

function newQueryWithDetails<TSchema extends Schema, TReturn extends QueryType>(
  delegate: QueryDelegate,
  schema: TSchema,
  ast: AST,
  format: Format | undefined,
): Query<TSchema, TReturn> {
  return new QueryImpl(delegate, schema, ast, format);
}

export type CommitListener = () => void;
export interface QueryDelegate extends BuilderDelegate {
  addServerQuery(ast: AST): () => void;
  onTransactionCommit(cb: CommitListener): () => void;
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
  TSchema extends Schema,
  TReturn extends QueryType = DefaultQueryResultRow<TSchema>,
> implements Query<TSchema, TReturn>
{
  readonly #ast: AST;
  readonly #schema: TSchema;
  readonly #format: Format;

  constructor(
    schema: TSchema,
    ast?: AST | undefined,
    format?: Format | undefined,
  ) {
    this.#ast = ast ?? {
      table: schema.tableName,
    };
    this.#format = format ?? {singular: false, relationships: {}};
    this.#schema = schema;
  }

  get ast() {
    return this.#ast;
  }

  select<TFields extends Selector<TSchema>[]>(
    ..._fields: TFields
  ): Query<TSchema, AddSelections<TSchema, TFields, TReturn>> {
    // we return all columns for now so we ignore the selection set and only use it for type inference
    return this._newQuery(this.#schema, this.#ast, this.#format);
  }

  protected abstract _newQuery<
    TSchema extends Schema,
    TReturn extends QueryType,
  >(
    schema: TSchema,
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
      const destSchema = resolveSchema(related1.dest.schema);
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
              subquery: addPrimaryKeysToAst(destSchema, sq.ast),
            },
          ],
        },
        {
          ...this.#format,
          relationships: {
            ...this.#format.relationships,
            [relationship as string]: sq.format,
          },
        },
      );
    }

    if (isJunctionRelationship(related2)) {
      const destSchema = resolveSchema(related2.dest.schema);
      const junctionSchema = resolveSchema(related2.junction.schema);
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
                    subquery: addPrimaryKeysToAst(destSchema, sq.ast),
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
            [relationship as string]: sq.format,
          },
        },
      );
    }
    throw new Error(`Invalid relationship ${relationship as string}`);
  }

  where(
    field: any,
    opOrValue:
      | Operator
      | GetFieldTypeNoNullOrUndefined<any, any, any>
      | Parameter<any, any, any>,
    value?:
      | GetFieldTypeNoNullOrUndefined<any, any, any>
      | Parameter<any, any, any>,
  ): Query<TSchema, TReturn> {
    let op: Operator;
    if (value === undefined) {
      value = opOrValue;
      op = '=';
    } else {
      op = opOrValue as Operator;
    }

    return this._newQuery(
      this.#schema,
      {
        ...this.#ast,
        where: [
          ...(this.#ast.where ?? []),
          {
            type: 'simple',
            op,
            field: field as string,
            value,
          },
        ],
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

  protected _completeAst(): AST {
    const finalOrderBy = addPrimaryKeys(this.#schema, this.#ast.orderBy);
    if (this.#ast.start) {
      const {row} = this.#ast.start;
      const narrowedRow: Row = {};
      for (const [field] of finalOrderBy) {
        narrowedRow[field] = row[field];
      }
      return {
        ...this.#ast,
        start: {
          ...this.#ast.start,
          row: narrowedRow,
        },
        orderBy: finalOrderBy,
      };
    }
    return {
      ...this.#ast,
      orderBy: addPrimaryKeys(this.#schema, this.#ast.orderBy),
    };
  }

  abstract materialize(): TypedView<Smash<TReturn>>;
  abstract preload(): {
    cleanup: () => void;
  };
}

export class QueryImpl<
  TSchema extends Schema,
  TReturn extends QueryType = DefaultQueryResultRow<TSchema>,
> extends AbstractQuery<TSchema, TReturn> {
  readonly #delegate: QueryDelegate;
  readonly #format: Format;

  constructor(
    delegate: QueryDelegate,
    schema: TSchema,
    ast?: AST | undefined,
    format?: Format | undefined,
  ) {
    super(schema, ast, format);
    this.#format = format ?? {singular: false, relationships: {}};
    this.#delegate = delegate;
  }

  get format() {
    return this.#format;
  }

  get singular(): TReturn['singular'] {
    return this.#format.singular;
  }

  protected _newQuery<TSchema extends Schema, TReturn extends QueryType>(
    schema: TSchema,
    ast: AST,
    format: Format | undefined,
  ): Query<TSchema, TReturn> {
    return newQueryWithDetails(this.#delegate, schema, ast, format);
  }

  materialize(): TypedView<Smash<TReturn>> {
    const ast = this._completeAst();
    const removeServerQuery = this.#delegate.addServerQuery(ast);
    const view = new ArrayView(
      buildPipeline(ast, this.#delegate, undefined),
      this.#format,
    );
    const removeCommitObserver = this.#delegate.onTransactionCommit(() => {
      view.flush();
    });
    view.onDestroy = () => {
      removeCommitObserver();
      removeServerQuery();
    };
    return view as unknown as TypedView<Smash<TReturn>>;
  }

  preload(): {
    cleanup: () => void;
  } {
    const ast = this._completeAst();
    const unsub = this.#delegate.addServerQuery(ast);
    return {
      cleanup: unsub,
    };
  }
}

function resolveSchema(maybeSchema: Schema | Lazy<Schema>): Schema {
  if (typeof maybeSchema === 'function') {
    return maybeSchema();
  }

  return maybeSchema;
}

function addPrimaryKeys(
  schema: Schema,
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

function addPrimaryKeysToAst(schema: Schema, ast: AST): AST {
  return {
    ...ast,
    orderBy: addPrimaryKeys(schema, ast.orderBy),
  };
}
