import {assert} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Schema as ZeroSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {Format} from '../ivm/view.ts';
import type {CustomQueryID} from './named.ts';
import type {NoContext, Query} from './query.ts';

export const queryInternalsTag = Symbol('QueryInternals');

/**
 * Internal interface for query implementation details.
 * This is not part of the public API and should only be accessed via
 * the {@linkcode asQueryInternals} function.
 *
 * @typeParam TSchema The database schema type extending ZeroSchema
 * @typeParam TTable The name of the table being queried, must be a key of TSchema['tables']
 * @typeParam TReturn The return type of the query, defaults to PullRow<TTable, TSchema>
 */
export interface QueryInternals<
  TSchema extends ZeroSchema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext = NoContext,
> {
  readonly [queryInternalsTag]: true;

  /**
   * Format is used to specify the shape of the query results. This is used by
   * {@linkcode one} and it also describes the shape when using
   * {@linkcode related}.
   */
  readonly format: Format;

  /**
   * A string that uniquely identifies this query. This can be used to determine
   * if two queries are the same.
   *
   * The hash of a custom query, on the client, is the hash of its AST.
   * The hash of a custom query, on the server, is the hash of its name and args.
   *
   * The first allows many client-side queries to be pinned to the same backend query.
   * The second ensures we do not invoke a named query on the backend more than once for the same `name:arg` pairing.
   *
   * If the query.hash was of `name:args` then `useQuery` would de-dupe
   * queries with divergent ASTs.
   *
   * QueryManager will hash based on `name:args` since it is speaking with
   * the server which tracks queries by `name:args`.
   */
  hash(): string;

  readonly ast: AST;
  readonly completedAST: AST;

  readonly customQueryID: CustomQueryID | undefined;

  // withDelegate(
  //   delegate: QueryDelegate,
  // ): Query<TSchema, TTable, TReturn, TContext>;

  /**
   * Associates a name and arguments with this query for custom query tracking.
   * This is used internally to track named queries on the server.
   *
   * @internal
   */
  nameAndArgs(
    name: string,
    args: ReadonlyArray<ReadonlyJSONValue>,
  ): Query<TSchema, TTable, TReturn, TContext>;

  // withContext(
  //   ctx: TContext,
  // ): QueryInternals<TSchema, TTable, TReturn, TContext>;
}

export function asQueryInternals<
  TSchema extends ZeroSchema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
>(
  query: Query<TSchema, TTable, TReturn, TContext>,
): QueryInternals<TSchema, TTable, TReturn, TContext> {
  assert(queryInternalsTag in query);
  return query as unknown as QueryInternals<TSchema, TTable, TReturn, TContext>;
}
