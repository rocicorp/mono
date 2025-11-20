/* oxlint-disable @typescript-eslint/no-explicit-any */
import type {HumanReadable, Query, RunOptions} from './query.ts';
import type {Schema as ZeroSchema} from '../../../zero-types/src/schema.ts';
import type {PullRow} from './query.ts';

/**
 * A query that can be executed directly via its `.run()` method.
 *
 * RunnableQuery extends Query and is returned when queries are created
 * through a Zero instance (e.g., `zero.query.user.where(...)`).
 *
 * This allows for direct execution:
 * ```ts
 * const users = await zero.query.user.where('age', '>', 18).run();
 * ```
 *
 * All query builder methods (where, limit, orderBy, etc.) return `this`,
 * preserving the RunnableQuery type through method chaining.
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
   * @param options Execution options (wait for server, ttl, etc.)
   * @returns Promise resolving to query results
   */
  run(options?: RunOptions): Promise<HumanReadable<TReturn>>;
}
