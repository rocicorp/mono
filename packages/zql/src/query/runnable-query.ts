import {assert} from '../../../shared/src/asserts.ts';
import type {Schema as ZeroSchema} from '../../../zero-types/src/schema.ts';
import type {HumanReadable, Query, RunOptions} from './query.ts';

export interface RunnableQuery<
  TSchema extends ZeroSchema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
> extends Query<TSchema, TTable, TReturn, TContext> {
  /**
   * Executes the query and returns the result once. The `options` parameter
   * specifies whether to wait for complete results or return immediately,
   * and the time to live for the query.
   *
   * For server queries, this will always wait for complete results.
   *
   * @example
   * ```js
   * const result = await query.run({type: 'complete', ttl: '1m'});
   * ```
   */
  run(options?: RunOptions): Promise<HumanReadable<TReturn>>;
}

export function asRunnableQuery<
  TSchema extends ZeroSchema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
>(
  query: Query<TSchema, TTable, TReturn, TContext>,
): RunnableQuery<TSchema, TTable, TReturn, TContext> {
  assert(
    'run' in query && typeof query.run === 'function',
    'Not a RunnableQuery',
  );
  return query as RunnableQuery<TSchema, TTable, TReturn, TContext>;
}
