import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {ADHOC_QUERY_NAME} from '../../../zero-protocol/src/adhoc-queries.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {asQueryInternals} from './query-internals.ts';
import type {Query} from './query.ts';

/**
 * Wraps a query as an ad-hoc query for server execution.
 *
 * Ad-hoc queries allow you to send arbitrary ZQL queries to the server
 * without pre-defining named queries. The query's AST is passed to the
 * server's custom query handler where it can be validated and executed.
 *
 * @example
 * ```typescript
 * import {Zero, adhoc} from '@rocicorp/zero';
 *
 * const z = new Zero({...});
 *
 * // Ad-hoc query without pre-definition
 * const query = z.query.issue.where('status', 'open').related('creator');
 * const [issues] = useQuery(adhoc(query));
 * ```
 *
 * On the server, handle ad-hoc queries with `executeAdhocQuery`:
 *
 * @example
 * ```typescript
 * import {handleQueryRequest, executeAdhocQuery, ADHOC_QUERY_NAME} from '@rocicorp/zero/server';
 *
 * handleQueryRequest((name, args) => {
 *   if (name === ADHOC_QUERY_NAME) {
 *     return executeAdhocQuery(args, {schema});
 *   }
 *   return queries[name].fn({args, ctx: {}});
 * }, schema, req);
 * ```
 */
export function adhoc<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn,
>(query: Query<TTable, TSchema, TReturn>): Query<TTable, TSchema, TReturn> {
  const internals = asQueryInternals(query);
  const args: ReadonlyJSONValue[] = [{ast: internals.ast}];
  return internals.nameAndArgs(ADHOC_QUERY_NAME, args);
}

export {ADHOC_QUERY_NAME} from '../../../zero-protocol/src/adhoc-queries.ts';
