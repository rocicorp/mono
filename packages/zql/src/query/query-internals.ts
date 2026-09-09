import {assert} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Schema as ZeroSchema} from '../../../zero-types/src/schema.ts';
import type {Format} from '../ivm/view.ts';
import type {CustomQueryID} from './named.ts';
import type {Query} from './query.ts';

export const queryInternalsTag = Symbol();

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
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends ZeroSchema,
  TReturn,
> {
  readonly [queryInternalsTag]: true;

  /**
   * Format is used to specify the shape of the query results. This is used by
   * {@linkcode one} and it also describes the shape when using
   * {@linkcode related}.
   */
  readonly format: Format;

  /**
   * A string that uniquely identifies this query object: two queries share it
   * exactly when they are interchangeable, down to how their builder methods
   * behave.
   *
   * It covers the AST, the {@linkcode format} the results take, the system the
   * query was built for, the junction relationship it is being built inside,
   * and, for a custom query, the name and arguments it was declared with.
   * Everything past the AST is hashed explicitly because it leaves no trace
   * there: {@linkcode nameAndArgs} reuses the AST it is given, a query with no
   * relationship and no `exists` has nowhere to stamp its system, and the
   * junction flag -- which decides whether `limit` and `orderBy` throw --
   * appears nowhere in the AST at all. Without them, such queries would share a
   * hash and anything keyed by it would conflate them.
   *
   * It does *not* cover the schema, which has no stable identity to hash.
   * Anything keyed by this that could see more than one schema has to scope
   * itself by schema as well.
   *
   * This is entirely client-side and never reaches the server. The server knows
   * a custom query by the hash of its name and args, and any other query by the
   * hash of its AST alone -- see `materializeImpl`.
   */
  hash(): string;

  /**
   * The completed AST for this query, with any missing primary keys added to
   * orderBy and start.
   *
   * A QueryImpl hands out a `NormalizedAST`; this stays an `AST` so that the
   * ASTs derived from it (e.g. by binding static parameters) still fit.
   */
  readonly ast: AST;

  readonly customQueryID: CustomQueryID | undefined;

  /**
   * Associates a name and arguments with this query for custom query tracking.
   * This is used internally to track named queries on the server.
   *
   * @internal
   */
  nameAndArgs(
    name: string,
    args: ReadonlyArray<ReadonlyJSONValue>,
  ): Query<TTable, TSchema, TReturn>;
}

/**
 * Helper function to cast a Query to QueryInternals.
 * This is used in tests and internal code to access query implementation details.
 * The function asserts that the query has the queryInternalsTag to ensure it
 * properly implements the QueryInternals interface.
 *
 * @internal
 */
export function asQueryInternals<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends ZeroSchema,
  TReturn,
>(
  query: Query<TTable, TSchema, TReturn>,
): QueryInternals<TTable, TSchema, TReturn> {
  assert(
    queryInternalsTag in query,
    'Query does not implement QueryInternals. This likely means there are two copies of Zero in your runtime.',
  );
  return query as unknown as QueryInternals<TTable, TSchema, TReturn>;
}

export function isQueryInternals<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends ZeroSchema,
  TReturn,
>(obj: unknown): obj is QueryInternals<TTable, TSchema, TReturn> {
  return typeof obj === 'object' && obj !== null && queryInternalsTag in obj;
}

export function asQuery<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends ZeroSchema,
  TReturn,
>(
  queryInternals: QueryInternals<TTable, TSchema, TReturn>,
): Query<TTable, TSchema, TReturn> {
  assert(queryInternalsTag in queryInternals, 'Expected query internals tag');
  return queryInternals as unknown as Query<TTable, TSchema, TReturn>;
}

// oxlint-disable-next-line no-explicit-any
export type AnyQueryInternals = QueryInternals<string, ZeroSchema, any>;
