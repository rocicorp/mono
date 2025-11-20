// oxlint-disable no-explicit-any
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {
  AddSubreturn,
  AvailableRelationships,
  DestRow,
  HumanReadable,
  PullRow,
  Query,
  QueryReturn,
  RelatedCallback,
  RunOptions,
} from './query.ts';

export interface RunnableQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> extends Query<TSchema, TTable, TReturn> {
  run(options?: RunOptions): Promise<HumanReadable<TReturn>>;

  one(): RunnableQuery<TSchema, TTable, TReturn | undefined>;

  related<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
  ): RunnableQuery<
    TSchema,
    TTable,
    AddSubreturn<
      TReturn,
      DestRow<TSchema, TTable, TRelationship>,
      TRelationship
    >
  >;
  related<
    TRelationship extends AvailableRelationships<TTable, TSchema>,
    TSub extends Query<TSchema, string, any>,
  >(
    relationship: TRelationship,
    cb: RelatedCallback<TSchema, TTable, TRelationship, TSub>,
  ): RunnableQuery<
    TSchema,
    TTable,
    AddSubreturn<TReturn, QueryReturn<TSub>, TRelationship>
  >;
}
