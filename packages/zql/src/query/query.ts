/* eslint-disable @typescript-eslint/ban-types */

import type {Row} from '../../../zero-protocol/src/data.js';
import type {Source} from '../ivm/source.js';
import type {GenericCondition} from './expression.js';
import type {
  PullSchemaForRelationship,
  TableSchema,
  SchemaValueToTSType,
  TableSchemaToRow,
} from '../../../zero-schema/src/table-schema.js';
import type {TypedView} from './typed-view.js';
import type {Expand} from '../../../shared/src/expand.js';

/**
 * The type that can be passed into `select()`. A selector
 * references a field on an row.
 */
export type Selector<E extends TableSchema> = keyof E['columns'];
export type NoJsonSelector<T extends TableSchema> = Exclude<
  Selector<T>,
  JsonSelectors<T>
>;
type JsonSelectors<E extends TableSchema> = {
  [K in keyof E['columns']]: E['columns'][K] extends {type: 'json'} ? K : never;
}[keyof E['columns']];

export type Context = {
  getSource: (name: string) => Source;
  createStorage: () => Storage;
};

export type Smash<T extends QueryType> = T['singular'] extends true
  ? SmashOne<T> | undefined
  : Array<SmashOne<T>>;

type SmashOne<T extends QueryType> = Expand<
  T['row'] & {
    [K in keyof T['related']]: T['related'][K] extends QueryType
      ? Smash<T['related'][K]>
      : never;
  }
>;

export type GetFieldTypeNoNullOrUndefined<
  TSchema extends TableSchema,
  TColumn extends keyof TSchema['columns'],
  TOperator extends Operator,
> = TOperator extends 'IN' | 'NOT IN'
  ? Exclude<
      SchemaValueToTSType<TSchema['columns'][TColumn]>,
      null | undefined
    >[]
  : Exclude<SchemaValueToTSType<TSchema['columns'][TColumn]>, null | undefined>;

export type QueryReturnType<T extends Query<TableSchema>> = T extends Query<
  TableSchema,
  infer TReturn
>
  ? Smash<TReturn>
  : never;

export type QueryRowType<T extends Query<TableSchema>> = T extends Query<
  TableSchema,
  infer TReturn
>
  ? SmashOne<TReturn>
  : never;

// Adds TSubquery to TReturn under the alias TAs.
export type AddSubselect<
  TSubquery extends Query<TableSchema>,
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
  TableSchema,
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

export type DefaultQueryResultRow<TSchema extends TableSchema> = {
  row: TableSchemaToRow<TSchema>;
  related: {};
  singular: false;
};
// eslint-disable-next-line @typescript-eslint/naming-convention
export type Parameter<T, TField extends keyof T, _TReturn = T[TField]> = {
  type: 'static';
  anchor: 'authData' | 'preMutationRow';
  field: TField;
};

export interface Query<
  TSchema extends TableSchema,
  TReturn extends QueryType = DefaultQueryResultRow<TSchema>,
> {
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

  where<
    TSelector extends NoJsonSelector<TSchema>,
    TOperator extends Operator,
    TParamAnchor = never,
    TParamField extends keyof TParamAnchor = never,
    TParamTypeBound extends GetFieldTypeNoNullOrUndefined<
      TSchema,
      TSelector,
      TOperator
    > = never,
  >(
    field: TSelector,
    op: TOperator,
    value:
      | GetFieldTypeNoNullOrUndefined<TSchema, TSelector, TOperator>
      | Parameter<TParamAnchor, TParamField, TParamTypeBound>,
  ): Query<TSchema, TReturn>;

  where<
    TSelector extends NoJsonSelector<TSchema>,
    TParamAnchor = never,
    TParamField extends keyof TParamAnchor = never,
    TParamTypeBound extends GetFieldTypeNoNullOrUndefined<
      TSchema,
      TSelector,
      '='
    > = never,
  >(
    field: TSelector,
    value:
      | GetFieldTypeNoNullOrUndefined<TSchema, TSelector, '='>
      | Parameter<TParamAnchor, TParamField, TParamTypeBound>,
  ): Query<TSchema, TReturn>;

  where(condition: GenericCondition<TSchema>): Query<TSchema, TReturn>;

  start(
    row: Partial<TableSchemaToRow<TSchema>>,
    opts?: {inclusive: boolean} | undefined,
  ): Query<TSchema, TReturn>;

  limit(limit: number): Query<TSchema, TReturn>;

  orderBy<TSelector extends Selector<TSchema>>(
    field: TSelector,
    direction: 'asc' | 'desc',
  ): Query<TSchema, TReturn>;

  one(): Query<TSchema, MakeSingular<TReturn>>;

  /**
   * Materialize the query. This will run the query and return a view that
   * will update as the query updates. Incremental updates are typically very
   * cheap.
   */
  materialize(): TypedView<Smash<TReturn>>;

  /**
   * Run the query one time and return the result. This should only be used
   * when a query is indeed needed only once. If the query will be run multiple
   * times, use `materialize()` instead, as it is far cheaper for the updates.
   */
  run(): Smash<TReturn>;

  preload(): {
    cleanup: () => void;
    complete: Promise<void>;
  };
}
