/* eslint-disable @typescript-eslint/no-explicit-any */
import type {Expand, ExpandRecursive} from '../../../shared/src/expand.js';
import type {Schema as ZeroSchema} from '../../../zero-schema/src/builder/schema-builder.js';
import type {
  LastInTuple,
  SchemaValueToTSType,
  TableSchema,
} from '../../../zero-schema/src/table-schema.js';
import type {ExpressionFactory, ParameterReference} from './expression.js';
import type {TypedView} from './typed-view.js';

type Selector<E extends TableSchema> = keyof E['columns'];
export type NoJsonSelector<T extends TableSchema> = Exclude<
  Selector<T>,
  JsonSelectors<T>
>;
type JsonSelectors<E extends TableSchema> = {
  [K in keyof E['columns']]: E['columns'][K] extends {type: 'json'} ? K : never;
}[keyof E['columns']];

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
  | 'ILIKE'
  | 'IS'
  | 'IS NOT';

export type GetFilterType<
  TSchema extends TableSchema,
  TColumn extends keyof TSchema['columns'],
  TOperator extends Operator,
> = TOperator extends 'IS' | 'IS NOT'
  ? // SchemaValueToTSType adds null if the type is optional, but we add null
    // no matter what for dx reasons. See:
    // https://github.com/rocicorp/mono/pull/3576#discussion_r1925792608
    SchemaValueToTSType<TSchema['columns'][TColumn]> | null
  : TOperator extends 'IN' | 'NOT IN'
  ? // We don't want to compare to null in where clauses because it causes
    // confusing results:
    // https://zero.rocicorp.dev/docs/reading-data#comparing-to-null
    Exclude<SchemaValueToTSType<TSchema['columns'][TColumn]>, null>[]
  : Exclude<SchemaValueToTSType<TSchema['columns'][TColumn]>, null>;

export type AvailableRelationships<
  TTable extends string,
  TSchema extends ZeroSchema,
> = keyof TSchema['relationships'][TTable] & string;

export type DestTableName<
  TTable extends string,
  TSchema extends ZeroSchema,
  TRelationship extends string,
> = LastInTuple<TSchema['relationships'][TTable][TRelationship]>['destSchema'];

type DestRow<
  TTable extends string,
  TSchema extends ZeroSchema,
  TRelationship extends string,
> = TSchema['relationships'][TTable][TRelationship][0]['cardinality'] extends 'many'
  ? PullRow<DestTableName<TTable, TSchema, TRelationship>, TSchema>
  : PullRow<DestTableName<TTable, TSchema, TRelationship>, TSchema> | undefined;

type AddSubreturn<TExistingReturn, TSubselectReturn, TAs extends string> = {
  readonly [K in TAs]: undefined extends TSubselectReturn
    ? TSubselectReturn
    : readonly TSubselectReturn[];
} extends infer TNewRelationship
  ? undefined extends TExistingReturn
    ? (Exclude<TExistingReturn, undefined> & TNewRelationship) | undefined
    : TExistingReturn & TNewRelationship
  : never;

export type PullTableSchema<
  TTable extends string,
  TSchemas extends ZeroSchema,
> = TSchemas['tables'][TTable];

export type PullRow<TTable extends string, TSchema extends ZeroSchema> = {
  readonly [K in keyof PullTableSchema<
    TTable,
    TSchema
  >['columns']]: SchemaValueToTSType<
    PullTableSchema<TTable, TSchema>['columns'][K]
  >;
};

export type Row<T extends TableSchema | Query<ZeroSchema, string>> =
  T extends TableSchema
    ? {
        readonly [K in keyof T['columns']]: SchemaValueToTSType<
          T['columns'][K]
        >;
      }
    : T extends Query<ZeroSchema, string, infer TReturn>
    ? TReturn
    : never;

export interface Query<
  TSchema extends ZeroSchema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> {
  related<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
  ): Query<
    TSchema,
    TTable,
    AddSubreturn<
      TReturn,
      DestRow<TTable, TSchema, TRelationship>,
      TRelationship
    >
  >;
  related<
    TRelationship extends AvailableRelationships<TTable, TSchema>,
    TSub extends Query<TSchema, string, any>,
  >(
    relationship: TRelationship,
    cb: (
      q: Query<
        TSchema,
        DestTableName<TTable, TSchema, TRelationship>,
        DestRow<TTable, TSchema, TRelationship>
      >,
    ) => TSub,
  ): Query<
    TSchema,
    TTable,
    AddSubreturn<
      TReturn,
      TSub extends Query<TSchema, string, infer TSubReturn>
        ? TSubReturn
        : never,
      TRelationship
    >
  >;

  where<
    TSelector extends NoJsonSelector<PullTableSchema<TTable, TSchema>>,
    TOperator extends Operator,
  >(
    field: TSelector,
    op: TOperator,
    value:
      | GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, TOperator>
      | ParameterReference,
  ): Query<TSchema, TTable, TReturn>;
  where<TSelector extends NoJsonSelector<PullTableSchema<TTable, TSchema>>>(
    field: TSelector,
    value:
      | GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, '='>
      | ParameterReference,
  ): Query<TSchema, TTable, TReturn>;
  where(
    expressionFactory: ExpressionFactory<TSchema, TTable>,
  ): Query<TSchema, TTable, TReturn>;

  whereExists(
    relationship: AvailableRelationships<TTable, TSchema>,
  ): Query<TSchema, TTable, TReturn>;
  whereExists<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
    cb: (
      q: Query<TSchema, DestTableName<TTable, TSchema, TRelationship>>,
    ) => Query<TSchema, string>,
  ): Query<TSchema, TTable, TReturn>;

  start(
    row: Partial<PullRow<TTable, TSchema>>,
    opts?: {inclusive: boolean} | undefined,
  ): Query<TSchema, TTable, TReturn>;

  limit(limit: number): Query<TSchema, TTable, TReturn>;

  orderBy<TSelector extends Selector<PullTableSchema<TTable, TSchema>>>(
    field: TSelector,
    direction: 'asc' | 'desc',
  ): Query<TSchema, TTable, TReturn>;

  one(): Query<TSchema, TTable, TReturn | undefined>;

  materialize(): TypedView<HumanReadable<TReturn>>;

  run(): HumanReadable<TReturn>;

  preload(): {
    cleanup: () => void;
    complete: Promise<void>;
  };
}

export type HumanReadable<T> = undefined extends T ? Expand<T> : Expand<T>[];
// Note: opaque types expand incorrectly.
export type HumanReadableRecursive<T> = undefined extends T
  ? ExpandRecursive<T>
  : ExpandRecursive<T>[];
