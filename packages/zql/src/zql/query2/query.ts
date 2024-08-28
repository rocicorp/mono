/* eslint-disable @typescript-eslint/ban-types */
import {AST} from '../ast2/ast.js';
import {Row} from '../ivm2/data.js';
import {Source} from '../ivm2/source.js';
import {Schema, PullSchemaForRelationship, SchemaValue} from './schema.js';
import {TypedView} from './typed-view.js';

/**
 * The type that can be passed into `select()`. A selector
 * references a field on an row.
 */
export type Selector<E extends Schema> = keyof E['fields'];

export type Context = {
  getSource: (name: string) => Source;
  createStorage: () => Storage;
};

export type Smash<T extends Array<QueryResultRow>> = {} & Array<
  T extends Array<infer TRow extends QueryResultRow>
    ? Collapse<
        TRow['row'] & {
          [K in keyof TRow['related']]: TRow['related'][K] extends Array<QueryResultRow>
            ? Smash<TRow['related'][K]>
            : undefined;
        }
      >
    : never
>;

type Collapse<T> = T extends object ? {[K in keyof T]: T[K]} : T;

/**
 * Given a schema value, return the TypeScript type.
 *
 * This allows us to create the correct return type for a
 * query that has a selection.
 */
type SchemaValueToTSType<T extends SchemaValue> =
  | (T extends {type: 'string'}
      ? string
      : T extends {type: 'number'}
      ? number
      : T extends {type: 'boolean'}
      ? boolean
      : T extends {type: 'null'}
      ? null
      : never)
  | (T extends {optional: true} ? undefined : never);

export type GetFieldTypeNoNullOrUndefined<
  TSchema extends Schema,
  TField extends keyof TSchema['fields'],
  TOperator extends Operator,
> = TOperator extends 'IN' | 'NOT IN'
  ? Exclude<SchemaValueToTSType<TSchema['fields'][TField]>, null | undefined>[]
  : Exclude<SchemaValueToTSType<TSchema['fields'][TField]>, null | undefined>;

export type SchemaToRow<T extends Schema> = {
  [K in keyof T['fields']]: SchemaValueToTSType<T['fields'][K]>;
};

export type QueryReturnType<T extends Query<Schema>> = T extends Query<
  Schema,
  infer TReturn
>
  ? Smash<TReturn>
  : never;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type QueryRowType<T extends Query<any, any, any>> =
  QueryReturnType<T>[number];

/**
 * A query can have:
 * 1. Selections and
 * 2. Subqueries
 *
 * The composition of these two yields the return type
 * of the query.
 *
 * This takes a return type of a query (TReturn), a schema type (TSchema),
 * and a list of selections (TSelections) made against that row,
 * returning a new return type with the selections added.
 *
 * `.select('foo')` would add `foo` to `TReturn`.
 */
export type AddSelections<
  TSchema extends Schema,
  TSelections extends Selector<TSchema>[],
  TReturn extends Array<QueryResultRow>,
> = {
  row: {
    [K in TSelections[number]]: SchemaValueToTSType<TSchema['fields'][K]>;
  };
  related: TReturn extends Array<infer TRow extends QueryResultRow>
    ? TRow['related']
    : {};
};

/**
 * Just like `AddSelections` but adds a subselect
 * to the return type (TReturn).
 */
export type AddSubselect<
  TSubquery extends Query<Schema>,
  TReturn extends Array<QueryResultRow>,
> = {
  row: TReturn extends Array<infer TRow extends QueryResultRow>
    ? TRow['row']
    : {};
  related: TReturn extends Array<infer TRow extends QueryResultRow>
    ? PickSubselect<TSubquery> & TRow['related']
    : PickSubselect<TSubquery>;
};

/**
 * Subselects have aliases which is how they're inserted
 * into the tree of values. This function takes a subquery
 * and infers its alias to be used in the return type.
 *
 * `sub(query => query.select('foo').as('bar'))` would
 * return `{bar: {foo: string}}`.
 */
export type PickSubselect<TSubquery extends Query<Schema>> = {
  [K in TSubquery extends Query<Schema, Array<QueryResultRow>, infer TAs>
    ? TAs
    : never]: TSubquery extends Query<Schema, infer TSubreturn>
    ? TSubreturn
    : EmptyQueryResultRow;
};

/**
 * The result of a ZQL query.
 *
 * Represents a tree of entities and subselects.
 */
export type QueryResultRow = {
  row: Partial<Row>;
  related: Record<string, Array<QueryResultRow>> | undefined;
};

type EmptyQueryResultRow = {
  row: {};
  related: {};
};

export type Operator =
  | '='
  | '!='
  | '<'
  | '<='
  | '>'
  | '>='
  | 'IN'
  | 'NOT IN'
  | 'LIKE'
  | 'ILIKE';

export type DefaultQueryResultRow<TSchema extends Schema> = {
  row: {
    [K in keyof TSchema['fields']]: SchemaValueToTSType<TSchema['fields'][K]>;
  };
  related: {};
};

export interface Query<
  TSchema extends Schema,
  TReturn extends Array<QueryResultRow> = Array<DefaultQueryResultRow<TSchema>>,
  TAs extends string = string,
> {
  readonly ast: AST;

  select<TFields extends Selector<TSchema>[]>(
    ...x: TFields
  ): Query<TSchema, Array<AddSelections<TSchema, TFields, TReturn>>, TAs>;

  as<TAs2 extends string>(as: TAs2): Query<TSchema, TReturn, TAs2>;

  related<TRelationship extends keyof TSchema['relationships']>(
    relationship: TRelationship,
  ): Query<
    TSchema,
    Array<
      AddSubselect<
        Query<
          PullSchemaForRelationship<TSchema, TRelationship>,
          Array<
            DefaultQueryResultRow<
              PullSchemaForRelationship<TSchema, TRelationship>
            >
          >,
          TRelationship & string
        >,
        TReturn
      >
    >,
    TAs
  >;
  related<
    TRelationship extends keyof TSchema['relationships'],
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    TSub extends Query<any, any, any>,
  >(
    relationship: TRelationship,
    cb: (
      query: Query<
        PullSchemaForRelationship<TSchema, TRelationship>,
        Array<
          DefaultQueryResultRow<
            PullSchemaForRelationship<TSchema, TRelationship>
          >
        >,
        TRelationship & string
      >,
    ) => TSub,
  ): Query<TSchema, Array<AddSubselect<TSub, TReturn>>, TAs>;

  where<TSelector extends Selector<TSchema>>(
    field: TSelector,
    op: Operator,
    value: GetFieldTypeNoNullOrUndefined<TSchema, TSelector, Operator>,
  ): Query<TSchema, TReturn, TAs>;

  start(
    row: Partial<SchemaToRow<TSchema>>,
    opts?: {inclusive: boolean} | undefined,
  ): Query<TSchema, TReturn, TAs>;

  limit(limit: number): Query<TSchema, TReturn, TAs>;

  orderBy<TSelector extends Selector<TSchema>>(
    field: TSelector,
    direction: 'asc' | 'desc',
  ): Query<TSchema, TReturn, TAs>;

  materialize(): TypedView<Smash<TReturn>>;
  preload(): {
    cleanup: () => void;
    preloaded: Promise<boolean>;
  };
}
