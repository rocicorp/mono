/**
 * Re-wrap a raw `AST` as a runnable `Query` (no PG dependency, so the fast no-PG tests
 * and the PG driver share one definition). A generated or shrunk `AST` is lowered through
 * the same {@link newQueryImpl} the fluent builder uses; the {@link Format} is derived
 * from the AST's relationship cardinality so the IVM and the oracle shape the output
 * identically (an imperfect derivation still keeps the differential comparison valid,
 * since both sides consume the same format).
 */

import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Format} from '../../../../zero-types/src/format.ts';
import {newQueryImpl} from '../../../../zql/src/query/query-impl.ts';
import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {schema} from '../schema.ts';
import {relsOf} from './axes.ts';

/**
 * Derive a {@link Format} from an `AST` (singular from relationship cardinality; junction
 * hidden hops collapsed, mirroring the view).
 */
export function deriveFormat(ast: AST, singular: boolean): Format {
  const relationships: Record<string, Format> = {};
  for (const r of ast.related ?? []) {
    if (r.hidden) {
      // Junction hop: the visible relationship lives one level down (the view collapses
      // the hidden level), so lift the child's relationships up.
      Object.assign(
        relationships,
        deriveFormat(r.subquery, false).relationships,
      );
      continue;
    }
    const name = r.subquery.alias;
    if (!name) {
      continue;
    }
    const one = relsOf(ast.table).find(rl => rl.name === name)?.card === 'one';
    relationships[name] = deriveFormat(r.subquery, one);
  }
  return {singular, relationships};
}

/** Re-wrap a raw `AST` as a runnable query (the IVM + oracle consume it identically). */
export function wrapAst(ast: AST, singular = false): AnyQuery {
  return newQueryImpl(
    schema,
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    ast.table as any,
    ast,
    deriveFormat(ast, singular),
    'test',
  ) as unknown as AnyQuery;
}
