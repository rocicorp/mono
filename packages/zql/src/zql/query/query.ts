/* eslint-disable @typescript-eslint/ban-types */
import {Row} from '../ivm/data.js';
import {SchemaValue} from '../ivm/schema.js';
import {Source} from '../ivm/source.js';
import {PullSchemaForRelationship, Schema} from './schema.js';
import {TypedView} from './typed-view.js';

/**
 * The type that can be passed into `select()`. A selector
 * references a field on an row.
 */
export type Selector<E extends Schema> = keyof E['columns'];

export type Context = {
  getSource: (name: string) => Source;
  createStorage: () => Storage;
};

export type Smash<T extends QueryType> = T['singular'] extends true
  ? SmashOne<T>
  : Array<SmashOne<T>>;

type SmashOne<T extends QueryType> = T['row'] & {
  [K in keyof T['related']]: T['related'][K] extends QueryType
    ? Smash<T['related'][K]>
    : never;
};

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
  TColumn extends keyof TSchema['columns'],
  TOperator extends Operator,
> = TOperator extends 'IN' | 'NOT IN'
  ? Exclude<
      SchemaValueToTSType<TSchema['columns'][TColumn]>,
      null | undefined
    >[]
  : Exclude<SchemaValueToTSType<TSchema['columns'][TColumn]>, null | undefined>;

export type SchemaToRow<T extends Schema> = {
  [K in keyof T['columns']]: SchemaValueToTSType<T['columns'][K]>;
};

export type QueryReturnType<T extends Query<Schema>> = T extends Query<
  Schema,
  infer TReturn
>
  ? Smash<TReturn>
  : never;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type QueryRowType<T extends Query<any, any>> =
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
  TReturn extends QueryType,
> = {
  row: {
    [K in TSelections[number]]: SchemaValueToTSType<TSchema['columns'][K]>;
  };
  related: TReturn['related'];
  singular: TReturn['singular'];
};

// Adds TSubquery to TReturn under the alias TAs.
export type AddSubselect<
  TSubquery extends Query<Schema>,
  TReturn extends QueryType,
  TAs extends string,
> = {
  row: TReturn['row'];
  related: {
    [K in TAs]: InferSubreturn<TSubquery>;
  } & TReturn['related'];
  singular: TReturn['singular'];
};

// Adds singular:true to TReturn.
export type MakeSingular<TReturn extends QueryType> = {
  row: TReturn['row'];
  related: TReturn['related'];
  singular: true;
};

type InferSubreturn<TSubquery> = TSubquery extends Query<
  Schema,
  infer TSubreturn
>
  ? TSubreturn
  : EmptyQueryResultRow;

/**
 * Encodes the internal "type" of the query. This is different than the schema,
 * and different than the result type. The schema is the input type from the
 * database of the table the query started from.
 *
 * The result type is the output type of the query after the 'row' and 'related'
 * fields have been smashed down.
 */
export type QueryType = {
  row: Row;
  related: Record<string, QueryType>;
  singular: boolean;
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
    [K in keyof TSchema['columns']]: SchemaValueToTSType<TSchema['columns'][K]>;
  };
  related: {};
  singular: false;
};

/** Expands/simplifies */
type Expand<T> = T extends infer O ? {[K in keyof O]: O[K]} : never;

export interface Query<
  TSchema extends Schema,
  TReturn extends QueryType = DefaultQueryResultRow<TSchema>,
> {
  select<TFields extends Selector<TSchema>[]>(
    ...columnName: Expand<TFields>
  ): Query<TSchema, AddSelections<TSchema, TFields, TReturn>>;

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
    ) => TSub,
  ): Query<TSchema, AddSubselect<TSub, TReturn, TRelationship & string>>;

  where<TSelector extends Selector<TSchema>, TOperator extends Operator>(
    field: TSelector,
    op: TOperator,
    value: GetFieldTypeNoNullOrUndefined<TSchema, TSelector, TOperator>,
  ): Query<TSchema, TReturn>;

  where<TSelector extends Selector<TSchema>>(
    field: TSelector,
    value: GetFieldTypeNoNullOrUndefined<TSchema, TSelector, '='>,
  ): Query<TSchema, TReturn>;

  start(
    row: Partial<SchemaToRow<TSchema>>,
    opts?: {inclusive: boolean} | undefined,
  ): Query<TSchema, TReturn>;

  limit(limit: number): Query<TSchema, TReturn>;

  orderBy<TSelector extends Selector<TSchema>>(
    field: TSelector,
    direction: 'asc' | 'desc',
  ): Query<TSchema, TReturn>;

  one(): Query<TSchema, MakeSingular<TReturn>>;

  materialize(): TypedView<Smash<TReturn>>;
  preload(): {
    cleanup: () => void;
  };
}
