import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import * as v from '../../shared/src/valita.ts';
import {
  adhocQueryArgSchema,
  ADHOC_QUERY_NAME,
  isAdhocQueryName,
} from '../../zero-protocol/src/adhoc-queries.ts';
import {defaultFormat} from '../../zero-types/src/format.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import {newQueryImpl} from '../../zql/src/query/query-impl.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';

export type AdhocQueryOptions<S extends Schema> = {
  schema: S;
  // Future: RLS context
  // authContext?: { authData: ReadonlyJSONValue };
};

/**
 * Server-side helper to execute ad-hoc queries.
 *
 * Ad-hoc queries allow clients to send raw AST queries without pre-defining
 * named queries. This function validates the incoming args and constructs
 * a Query object that can be returned from a custom query handler.
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
export function executeAdhocQuery<S extends Schema>(
  args: ReadonlyJSONValue | undefined,
  options: AdhocQueryOptions<S>,
): AnyQuery {
  const {ast} = v.parse(args, adhocQueryArgSchema);
  const tableName = ast.table as keyof S['tables'] & string;

  // Future: Apply RLS transformations here
  // const transformedAst = applyRLS(ast, options.authContext);

  return newQueryImpl(options.schema, tableName, ast, defaultFormat, 'client');
}

export {ADHOC_QUERY_NAME, isAdhocQueryName};
