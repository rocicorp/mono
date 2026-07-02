/**
 * **Scalar subqueries** (the one phase-7 axis deferred at first): a `whereExists(rel, …,
 * {scalar: true})` gate that z2s compiles to `parentField = (SELECT childField … LIMIT 1)`
 * but the base IVM does **not** natively resolve — only zero-cache's pipeline-driver does,
 * by pre-resolving "simple" (unique-key-constrained, ≤1-row) scalar subqueries to a literal
 * `parentField = <value>` condition via {@link resolveSimpleScalarSubqueries}.
 *
 * So a faithful differential mirrors production: run the **original** scalar AST through the
 * Postgres oracle (z2s), and run the IVM (memory + sqlite) over the **pre-resolved** AST —
 * the exact transform the pipeline-driver applies. To keep the subquery simple (so it
 * actually resolves), we constrain the child's single-column **primary key** to one present
 * value; the resolver then rewrites the gate to the literal it would in production.
 *
 * This module is PG-free (the generator + the IVM-side resolve). The differential itself —
 * original-through-PG vs resolved-through-IVM — lives in `driver.ts`'s `checkScalar`.
 */

import type {AST, LiteralValue} from '../../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {
  extractLiteralEqualityConstraints,
  resolveSimpleScalarSubqueries,
  type ScalarExecutor,
} from '../../../../zqlite/src/resolve-scalar-subqueries.ts';
import {pkOf, relsOf, tables} from './axes.ts';
import type {Data} from './literals.ts';
import {lower} from './skeleton.ts';

/** A scalar-subquery candidate: a one-hop relationship a simple scalar gate can sit on. */
export type ScalarCandidate = {
  readonly table: string;
  readonly rel: string;
  readonly child: string;
  readonly childPk: string;
};

/**
 * Every `(table, one-hop relationship)` pair a *simple* scalar subquery can be built on:
 * non-junction (the builder throws on two-hop junctions) and a single-column-PK child (so
 * the subquery is made simple by constraining that PK to one present value). Deterministic
 * in schema-declaration order.
 */
export function scalarCandidates(): ScalarCandidate[] {
  const out: ScalarCandidate[] = [];
  for (const table of tables()) {
    for (const rel of relsOf(table)) {
      if (rel.junction) {
        continue; // scalar is unsupported on two-hop junctions (the builder throws)
      }
      const pk = pkOf(rel.child);
      if (pk.length !== 1) {
        continue; // need a single-column PK to constrain the subquery to one row
      }
      out.push({table, rel: rel.name, child: rel.child, childPk: pk[0]});
    }
  }
  return out;
}

/**
 * Build the scalar-subquery query for `cand`:
 * `table.whereExists(rel, child ⇒ child.where(childPk = <present value>), {scalar: true})`.
 *
 * PK-constrained ⇒ the subquery returns ≤1 row, so it is *simple* and the production
 * resolver rewrites the gate to `parentField = <selected childField>`. `null` if the child
 * has no present PK value in `data` (an empty table).
 */
export function buildScalar(
  cand: ScalarCandidate,
  data: Data,
): AnyQuery | null {
  const pkVal = data.pkMid(cand.child);
  if (pkVal === undefined) {
    return null;
  }
  const base = lower({table: cand.table, children: []});
  return (
    base as unknown as {
      whereExists(
        rel: string,
        cb: (q: AnyQuery) => AnyQuery,
        opts: {scalar: boolean},
      ): AnyQuery;
    }
  ).whereExists(
    cand.rel,
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    (q: any) => q.where(cand.childPk, '=', pkVal),
    {scalar: true},
  );
}

/**
 * The `tableSpecs` map {@link resolveSimpleScalarSubqueries} needs, derived from the client
 * {@link schema}: each table's primary key is treated as a unique key — the subset of unique
 * keys the client schema records, and enough to make a PK-constrained subquery simple. Keyed
 * by **client** table name (matching the client-named subquery ASTs the builder produces).
 */
export function scalarTableSpecs(): Map<
  string,
  {tableSpec: {uniqueKeys: PrimaryKey[]}}
> {
  const m = new Map<string, {tableSpec: {uniqueKeys: PrimaryKey[]}}>();
  for (const t of tables()) {
    m.set(t, {tableSpec: {uniqueKeys: [pkOf(t) as unknown as PrimaryKey]}});
  }
  return m;
}

/**
 * A synchronous {@link ScalarExecutor} backed by the raw fixture rows. For a *simple*
 * subquery the equality constraints pin a unique-key row, so a direct lookup returns the
 * same `childField` value the IVM pipeline (or Postgres `… LIMIT 1`) would. Returns
 * `undefined` when no row matches (→ the resolver yields an always-false gate, matching
 * Postgres's empty `(SELECT …)`).
 */
export function makeScalarExecutor(
  rawRows: Record<string, readonly Row[]>,
): ScalarExecutor {
  return (subqueryAST: AST, childField: string) => {
    if (!subqueryAST.where) {
      return undefined;
    }
    const constraints = extractLiteralEqualityConstraints(subqueryAST.where);
    const rows = rawRows[subqueryAST.table] ?? [];
    const match = rows.find(r =>
      [...constraints].every(([col, val]) => eq(r[col], val)),
    );
    if (!match) {
      return undefined;
    }
    return (match[childField] ?? null) as LiteralValue | null;
  };
}

function eq(a: Value | undefined, b: LiteralValue): boolean {
  return a === b;
}

/**
 * Pre-resolve every simple scalar subquery in `ast` to a literal condition — the exact
 * transform zero-cache's pipeline-driver applies before running the IVM. The returned AST
 * has the scalar gate replaced by a plain `parentField = <value>` (or an always-false
 * condition), so the base IVM runs it as an ordinary filter.
 */
export function resolveScalarForIvm(
  ast: AST,
  rawRows: Record<string, readonly Row[]>,
): AST {
  return resolveSimpleScalarSubqueries(
    ast,
    scalarTableSpecs(),
    makeScalarExecutor(rawRows),
  ).ast;
}

/** Whether `ast` still contains a `scalar: true` correlated subquery (unresolved). */
export function hasScalarSubquery(ast: AST): boolean {
  const inCond = (c: AST['where']): boolean => {
    if (!c) {
      return false;
    }
    switch (c.type) {
      case 'correlatedSubquery':
        return !!c.scalar || inAst(c.related.subquery);
      case 'and':
      case 'or':
        return c.conditions.some(inCond);
      default:
        return false;
    }
  };
  const inAst = (a: AST): boolean =>
    inCond(a.where) || (a.related ?? []).some(r => inAst(r.subquery));
  return inAst(ast);
}
