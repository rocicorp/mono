import type {HumanReadable, Query, RunOptions} from './query.ts';
import type {Schema as ZeroSchema} from '../../../zero-types/src/schema.ts';
import type {PullRow} from './query.ts';

/**
 * A query that can be executed directly with a `.run()` method.
 *
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
  /**
   * Execute the query and return results.
   *
   * @param options - Options for running the query
   * @param options.type - 'unknown' (default) returns cached data immediately,
   *                       'complete' waits for server data
   * @param options.ttl - Time to live for keeping the query data after execution
   * @returns A promise that resolves to the query results
   */
  run(options?: RunOptions): Promise<HumanReadable<TReturn>>;
}
