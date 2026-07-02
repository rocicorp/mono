/**
 * **L3 — mutation-from-corpus** (ported from rusty-ivm `rindle-fuzz/src/mutate.rs`,
 * design §2 L3). Seed a corpus of small known-good queries (the bounded-exhaustive
 * skeletons) and apply **one** small mutation each: add a `where` conjunct, wrap in an
 * EXISTS, add `order`/`limit`, flip a direction. This keeps queries at "simple + one
 * twist" — exactly where bugs and minimal repros live — and biases hard toward small.
 *
 * Unlike the Rust port, mutation works on the **raw `AST`** (the corpus base is
 * `lower(skeleton)`'s AST). A `where`/`order`/`limit` twist needs no correlation, so it
 * splices directly; an `AddExists` twist DOES need correlation keys, so it borrows them
 * from a throwaway builder query ({@link existsConditionFor}) — the same path L1 uses —
 * rather than hand-rolling them. (The `start` twist is still separate; it needs a
 * data-driven cursor row to avoid mostly-vacuous mutations.)
 */

import {must} from '../../../../shared/src/must.ts';
import type {
  AST,
  Condition,
  Ordering,
} from '../../../../zero-protocol/src/ast.ts';
import {asQueryInternals} from '../../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {newStaticQuery} from '../../../../zql/src/query/static-query.ts';
import {schema} from '../schema.ts';
import {
  EXISTS_VALS,
  type ExistsVal,
  FILTER_VALS,
  filterRealizable,
  type Rel,
  relsOf,
  rolesOf,
} from './axes.ts';
import {existsCondition} from './cover.ts';
import {filterCondition} from './literals.ts';
import type {Rng} from './rng.ts';

type Twist = 'addFilter' | 'addExists' | 'addOrder' | 'addLimit' | 'flipDir';

/** The non-`none` EXISTS shapes an `AddExists` twist may introduce. */
const EXISTS_KINDS = EXISTS_VALS.filter((v): v is ExistsVal => v !== 'none');

/**
 * Apply **one** random small mutation to `ast` (using the root table's value roles),
 * yielding a "simple + one twist" query. A no-op only if the chosen twist turns out
 * unrealizable (no value, no free relationship). Pure — never mutates `ast`.
 */
export function mutate(rng: Rng, ast: AST): AST {
  const table = ast.table;
  // Relationship names already used at the root (a materialized `related` or an EXISTS
  // gate): an EXISTS must NOT reuse one (the builder allows one relationship per slot, so
  // a colliding alias is a build error, not a query).
  const used = usedRootRels(ast);

  const twists: Twist[] = ['addFilter'];
  if (relsOf(table).some(r => !used.has(r.name))) {
    twists.push('addExists');
  }
  if (ast.limit === undefined) {
    twists.push('addLimit');
  }
  if (!ast.orderBy || ast.orderBy.length === 0) {
    twists.push('addOrder');
  } else {
    twists.push('flipDir');
  }
  const twist = must(rng.choose(twists)); // addFilter is always applicable

  switch (twist) {
    case 'addFilter':
      return addRootCond(ast, pickFilter(rng, table));
    case 'addExists':
      return addRootCond(ast, pickExists(rng, table, used));
    case 'addOrder': {
      const dir: 'asc' | 'desc' = rng.bool() ? 'asc' : 'desc';
      return {...ast, orderBy: [[rolesOf(table).orderCol, dir]]};
    }
    case 'addLimit':
      return {...ast, limit: rng.bool() ? 2 : 10_000};
    case 'flipDir':
      return flipDir(rng, ast);
  }
}

/** A random realizable root filter condition, or `null`. */
function pickFilter(rng: Rng, table: string): Condition | null {
  const realizable = FILTER_VALS.filter(
    v => v !== 'none' && filterRealizable(table, v),
  );
  const v = rng.choose(realizable);
  return v ? filterCondition(table, v) : null;
}

/** A random EXISTS gate on a relationship NOT already used at the root, or `null`. */
function pickExists(
  rng: Rng,
  table: string,
  used: ReadonlySet<string>,
): Condition | null {
  const rel = rng.choose(relsOf(table).filter(r => !used.has(r.name)));
  const ev = rng.choose(EXISTS_KINDS);
  return rel && ev ? existsConditionFor(table, rel, ev) : null;
}

/**
 * The EXISTS condition for `ev` on `table.rel` **with its correlation keys filled** —
 * borrowed from a throwaway builder query (the builder owns the junction/correlation
 * lowering), so the spliced condition is identical to one L1 would have produced.
 */
function existsConditionFor(table: string, rel: Rel, ev: ExistsVal): Condition {
  const q = // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    (newStaticQuery(schema, table as any) as any)
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      .where((eb: any) => existsCondition(eb, table, rel, ev));
  return must(asQueryInternals(q as AnyQuery).ast.where);
}

/** AND a new condition into the root `where` (flattening an existing top-level AND). */
function addRootCond(ast: AST, c: Condition | null): AST {
  if (!c) {
    return ast;
  }
  const conds: Condition[] = !ast.where
    ? []
    : ast.where.type === 'and'
      ? [...ast.where.conditions]
      : [ast.where];
  conds.push(c);
  return {...ast, where: foldAnd(conds)};
}

function foldAnd(conds: readonly Condition[]): Condition | undefined {
  if (conds.length === 0) {
    return undefined;
  }
  if (conds.length === 1) {
    return conds[0];
  }
  return {type: 'and', conditions: conds};
}

/** Flip a random `orderBy` term's direction. A no-op if there is no order. */
function flipDir(rng: Rng, ast: AST): AST {
  const ob = ast.orderBy ?? [];
  if (ob.length === 0) {
    return ast;
  }
  const i = rng.int(ob.length);
  const orderBy: Ordering = ob.map((p, j) =>
    j === i ? [p[0], p[1] === 'asc' ? 'desc' : 'asc'] : p,
  );
  return {...ast, orderBy};
}

// ── used-relationship analysis (alias-collision avoidance) ────────────────────────────

/** Relationship names already used at the root: a materialized child or an EXISTS gate. */
function usedRootRels(ast: AST): Set<string> {
  const s = new Set<string>();
  for (const r of ast.related ?? []) {
    if (r.subquery.alias) {
      s.add(r.subquery.alias);
    }
  }
  if (ast.where) {
    collectExistsAliases(ast.where, s);
  }
  return s;
}

function collectExistsAliases(c: Condition, s: Set<string>): void {
  switch (c.type) {
    case 'simple':
      return;
    case 'correlatedSubquery':
      if (c.related.subquery.alias) {
        s.add(c.related.subquery.alias);
      }
      return;
    case 'and':
    case 'or':
      for (const cc of c.conditions) {
        collectExistsAliases(cc, s);
      }
  }
}
