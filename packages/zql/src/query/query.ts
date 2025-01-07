/* eslint-disable @typescript-eslint/no-explicit-any */
import type {
  LastInTuple,
  TableSchema,
} from '../../../zero-schema/src/table-schema.js';
import type {
  FullSchema,
  SchemaValueToTSType,
} from '../../../zero-schema/src/table-schema.js';
import type {ExpressionFactory, ParameterReference} from './expression.js';
import type {TypedView} from './typed-view.js';
import type {Expand} from '../../../shared/src/expand.js';

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

export type GetFieldTypeNoUndefined<
  TSchema extends TableSchema,
  TColumn extends keyof TSchema['columns'],
  TOperator extends Operator,
> = TOperator extends 'IN' | 'NOT IN'
  ? Exclude<
      SchemaValueToTSType<TSchema['columns'][TColumn]>,
      null | undefined
    >[]
  : TOperator extends 'IS' | 'IS NOT'
  ? Exclude<SchemaValueToTSType<TSchema['columns'][TColumn]>, undefined> | null
  : Exclude<SchemaValueToTSType<TSchema['columns'][TColumn]>, undefined>;

export type AvailableRelationships<
  TTable extends string,
  TSchema extends FullSchema,
> = keyof TSchema['relationships'][TTable] & string;

export type DestTableName<
  TTable extends string,
  TSchema extends FullSchema,
  TRelationship extends string,
> = LastInTuple<TSchema['relationships'][TTable][TRelationship]>['destSchema'];

type DestRow<
  TTable extends string,
  TSchema extends FullSchema,
  TRelationship extends string,
> = PullRow<DestTableName<TTable, TSchema, TRelationship>, TSchema>;

type AddSubreturn<
  TExistingReturn,
  TSubselectReturn,
  TAs extends string,
> = undefined extends TExistingReturn
  ?
      | (Exclude<TExistingReturn, undefined> & {
          readonly [K in TAs]: undefined extends TSubselectReturn
            ? TSubselectReturn
            : readonly TSubselectReturn[];
        })
      | undefined
  : TExistingReturn & {
      readonly [K in TAs]: undefined extends TSubselectReturn
        ? TSubselectReturn
        : readonly TSubselectReturn[];
    };

export type PullTableSchema<
  TTable extends string,
  TSchemas extends FullSchema,
> = TSchemas['tables'][TTable];

export type PullRow<TTable extends string, TSchema extends FullSchema> = {
  readonly [K in keyof PullTableSchema<
    TTable,
    TSchema
  >['columns']]: SchemaValueToTSType<
    PullTableSchema<TTable, TSchema>['columns'][K]
  >;
};

export type Row<T extends TableSchema | Query<FullSchema, string>> =
  T extends TableSchema
    ? {
        readonly [K in keyof T['columns']]: SchemaValueToTSType<
          T['columns'][K]
        >;
      }
    : T extends Query<FullSchema, string, infer TReturn>
    ? TReturn
    : never;

export interface Query<
  TSchema extends FullSchema,
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
      q: Query<TSchema, DestTableName<TTable, TSchema, TRelationship>>,
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
      | GetFieldTypeNoUndefined<
          PullTableSchema<TTable, TSchema>,
          TSelector,
          TOperator
        >
      | ParameterReference,
  ): Query<TSchema, TTable, TReturn>;
  where<TSelector extends NoJsonSelector<PullTableSchema<TTable, TSchema>>>(
    field: TSelector,
    value:
      | GetFieldTypeNoUndefined<
          PullTableSchema<TTable, TSchema>,
          TSelector,
          '='
        >
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

// Note: opaque types expand incorrectly.
export type HumanReadable<T> = undefined extends T ? Expand<T> : Expand<T>[];
