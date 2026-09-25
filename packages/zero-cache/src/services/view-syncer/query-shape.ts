// @circular-dep-ignore
import {astToZQL} from '../../../../ast-to-zql/src/ast-to-zql.ts';
import {h64} from '../../../../shared/src/hash.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';

/**
 * A query with its literal values redacted, for logging.
 */
export type QueryShape = {
  /**
   * The query as ZQL with every literal (and the `start` row) rendered as
   * `?`, so it can be logged without leaking row data, query arguments, or
   * auth data substituted in by permissions.
   */
  readonly zql: string;

  /**
   * A hash of {@link zql}. Queries that differ only in their literal values,
   * e.g. the same named query called with different arguments, share a hash,
   * which makes it a key for grouping and throttling logs.
   */
  readonly hash: string;
};

/**
 * Never throws: it is computed for every hydration, and a query that cannot be
 * rendered as ZQL (e.g. a subquery without an alias) must still hydrate. Such
 * a query is shaped by its AST with the literal values redacted instead.
 */
export function queryShape(ast: AST): QueryShape {
  let zql: string;
  try {
    zql = ast.table + astToZQL(ast, {redactLiterals: true});
  } catch {
    zql = JSON.stringify(ast, redactLiterals);
  }
  return {zql, hash: h64(zql).toString(36)};
}

function redactLiterals(this: unknown, key: string, value: unknown) {
  if (
    (key === 'value' && (this as {type?: unknown}).type === 'literal') ||
    key === 'start'
  ) {
    return '?';
  }
  return value;
}
