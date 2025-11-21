// oxlint-disable no-explicit-any
import type {
  Query,
  RunOptions,
  AvailableRelationships,
  DestTableName,
  HumanReadableRecursive,
  AddSubreturn,
  DestRow,
} from './query.ts';
import type {Schema as ZeroSchema} from '../../../zero-types/src/schema.ts';
import type {PullRow} from './query.ts';

/**
 * A query that can be executed directly with a `.run()` method.
 * This is the type returned by `zero.query.tableName` which includes
 * the ability to run the query inline:
 *
 * ```typescript
 * const users = await zero.query.user
 *   .where('age', '>', 18)
 *   .limit(10)
 *   .run();
 * ```
 *
 * @typeParam TSchema The database schema type extending ZeroSchema
 * @typeParam TTable The name of the table being queried
 * @typeParam TReturn The return type of the query
 */
export interface RunnableQuery<
  TSchema extends ZeroSchema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> extends Query<TSchema, TTable, TReturn> {
  // Override related() to return RunnableQuery instead of Query
  related<TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
  ): RunnableQuery<
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
  ): RunnableQuery<
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

  /**
   * Execute the query and return results.
   *
   * @param options - Options for running the query
   * @param options.type - 'unknown' (default) returns cached data immediately,
   *                       'complete' waits for server data
   * @param options.ttl - Time to live for keeping the query data after execution
   * @returns A promise that resolves to the query results
   */
  run(options?: RunOptions): Promise<HumanReadableRecursive<TReturn>>;
}
