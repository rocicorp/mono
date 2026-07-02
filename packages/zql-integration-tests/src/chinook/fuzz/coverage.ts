/**
 * **Coverage measurement** (ported from rusty-ivm `rindle-fuzz/src/coverage.rs`,
 * design §4) — making "we covered the shapes" a number.
 *
 * Two complementary instruments:
 *
 * 1. {@link tags} — a pure walk of an `AST` producing the set of **feature-interaction
 *    tags** it exhibits (`exists_under_or`, `order_desc_nullable`,
 *    `multi_col_correlation`, `related@depth1`, …). The union of tags over a sweep is
 *    the human-meaningful "which shapes did we exercise" report.
 * 2. {@link Coverage} — the **t-way report** over the formal decoration `AXES`: given
 *    the assignments actually generated, which `(axis, value)` t-tuples were hit and
 *    which were **missed**. The missed set drives the feedback loop and the backbone's
 *    "100% pairwise" assertion (design §11 phase 3).
 */

import type {
  AST,
  Condition,
  CorrelatedSubquery,
} from '../../../../zero-protocol/src/ast.ts';
import {AXES, columnsOf, hasColumn, N_AXES} from './axes.ts';

// ── combinatorics (shared with the covering-array builder) ────────────────────────────

/** All size-`k` index subsets of `0..n`, ascending (lexicographic next-combination). */
export function axisCombinations(n: number, k: number): number[][] {
  const out: number[][] = [];
  if (k === 0 || k > n) {
    return out;
  }
  const idx = Array.from({length: k}, (_, i) => i);
  for (;;) {
    out.push([...idx]);
    let i = k;
    for (;;) {
      if (i === 0) {
        return out;
      }
      i -= 1;
      if (idx[i] !== i + n - k) {
        break;
      }
    }
    idx[i] += 1;
    for (let j = i + 1; j < k; j++) {
      idx[j] = idx[j - 1] + 1;
    }
  }
}

/** The cartesian product of `0..d` for each domain size `d` in `domains`. */
export function cartesian(domains: readonly number[]): number[][] {
  let out: number[][] = [[]];
  for (const d of domains) {
    const next: number[][] = [];
    for (const prefix of out) {
      for (let v = 0; v < d; v++) {
        next.push([...prefix, v]);
      }
    }
    out = next;
  }
  return out;
}

// ── the t-way coverage report ─────────────────────────────────────────────────────────

/** A hit `(axis, value)` t-tuple, encoded as a sorted-by-axis string key. */
function tupleKey(tuple: ReadonlyArray<readonly [number, number]>): string {
  return tuple.map(([a, v]) => `${a}:${v}`).join(',');
}

/**
 * Accumulates which t-tuples over the formal `AXES` the generated assignments hit.
 * Coverage = `hit / total`; the **missed** set drives the feedback loop and the
 * backbone's "100% pairwise" assertion.
 */
export class Coverage {
  readonly #t: number;
  readonly #domains: number[];
  readonly #hit = new Set<string>();

  /** A report over the `AXES` for strength `t` (2 = pairwise). */
  constructor(t: number) {
    if (t < 1 || t > N_AXES) {
      throw new Error(`t out of range: ${t}`);
    }
    this.#t = t;
    this.#domains = AXES.map(a => a.values.length);
  }

  /**
   * Record one **full assignment** (a value index per axis, length `N_AXES`): every
   * one of its t-axis sub-tuples becomes hit.
   */
  observe(assignment: readonly number[]): void {
    if (assignment.length !== N_AXES) {
      throw new Error('assignment must cover every axis');
    }
    for (const combo of axisCombinations(N_AXES, this.#t)) {
      const tuple = combo.map(a => {
        if (assignment[a] >= this.#domains[a]) {
          throw new Error('value out of axis domain');
        }
        return [a, assignment[a]] as const;
      });
      this.#hit.add(tupleKey(tuple));
    }
  }

  /** Total coverable t-tuples (Σ over t-axis subsets of value combinations). */
  total(): number {
    let n = 0;
    for (const combo of axisCombinations(N_AXES, this.#t)) {
      n += combo.reduce((acc, a) => acc * this.#domains[a], 1);
    }
    return n;
  }

  /** How many distinct t-tuples have been hit. */
  hitCount(): number {
    return this.#hit.size;
  }

  /** Fraction in `[0, 1]` of coverable t-tuples hit. */
  fraction(): number {
    const total = this.total();
    return total === 0 ? 1 : this.hitCount() / total;
  }

  /** The t-tuples **not** yet hit, as `(axisName, valueName)` pairs. */
  missed(): Array<Array<[string, string]>> {
    const out: Array<Array<[string, string]>> = [];
    for (const combo of axisCombinations(N_AXES, this.#t)) {
      const sizes = combo.map(a => this.#domains[a]);
      for (const values of cartesian(sizes)) {
        const tuple = combo.map((a, i) => [a, values[i]] as const);
        if (!this.#hit.has(tupleKey(tuple))) {
          out.push(
            tuple.map(
              ([a, v]) => [AXES[a].name, AXES[a].values[v]] as [string, string],
            ),
          );
        }
      }
    }
    return out;
  }

  /** A one-line human summary (`2-way: 441/441 (100.0%)`). */
  summary(): string {
    return `${this.#t}-way: ${this.hitCount()}/${this.total()} (${(
      this.fraction() * 100
    ).toFixed(1)}%)`;
  }
}

// ── feature tags (a pure AST walk) ────────────────────────────────────────────────────

type Pos = 'top' | 'under_and' | 'under_or';

/**
 * The set of feature-interaction tags `ast` exhibits. Tags are stable strings so a
 * sweep can union them and diff against a target.
 */
export function tags(ast: AST): Set<string> {
  const t = new Set<string>();
  if (countExists(ast) >= 2) {
    t.add('multi_exists');
  }
  t.add(`max_depth:${maxDepth(ast)}`);
  walk(ast, 0, t);
  return t;
}

function walk(ast: AST, depth: number, t: Set<string>): void {
  if (ast.limit !== undefined) {
    t.add('limit');
    if (ast.orderBy && ast.orderBy.length > 0) {
      t.add('limit+order');
    }
    if (ast.start !== undefined) {
      t.add('limit+start');
    }
  }
  orderTags(ast, t);
  startTags(ast, t);

  if (ast.where) {
    condTags(ast.table, ast.where, depth, 'top', t);
  }

  for (const r of ast.related ?? []) {
    t.add(`related@depth${depth + 1}`);
    corrTags(ast.table, r, t);
    walk(r.subquery, depth + 1, t);
  }
}

function orderTags(ast: AST, t: Set<string>): void {
  const orderBy = ast.orderBy ?? [];
  if (orderBy.length === 0) {
    return;
  }
  if (orderBy.length >= 2) {
    t.add('order:multi');
  }
  for (const [field, dir] of orderBy) {
    if (dir === 'asc') {
      t.add('order:asc');
    } else {
      t.add('order:desc');
      if (hasColumn(ast.table, field)) {
        const col = columnsOf(ast.table).find(c => c.name === field);
        if (col?.optional) {
          t.add('order_desc_nullable');
        }
      }
    }
  }
}

function startTags(ast: AST, t: Set<string>): void {
  if (ast.start !== undefined) {
    t.add(ast.start.exclusive ? 'start:after' : 'start:at');
    if (Object.keys(ast.start.row).length >= 2) {
      t.add('start:multikey');
    }
  }
}

function condTags(
  table: string,
  c: Condition,
  depth: number,
  pos: Pos,
  t: Set<string>,
): void {
  switch (c.type) {
    case 'simple':
      t.add(`filter:${c.op}`);
      break;
    case 'and':
      t.add('where:and');
      for (const cc of c.conditions) {
        condTags(table, cc, depth, 'under_and', t);
      }
      break;
    case 'or':
      t.add('where:or');
      for (const cc of c.conditions) {
        condTags(table, cc, depth, 'under_or', t);
      }
      break;
    case 'correlatedSubquery': {
      const kind = c.op === 'EXISTS' ? 'exists' : 'not_exists';
      t.add(kind);
      t.add(`${kind}@depth${depth}`);
      t.add(`${kind}_${pos}`);
      corrTags(table, c.related, t);
      if (c.related.subquery.where) {
        condTags(
          c.related.subquery.table,
          c.related.subquery.where,
          depth + 1,
          'top',
          t,
        );
      }
      break;
    }
  }
}

/** Tags for a correlation hop (shared by `related` and EXISTS): self-join, multi-col. */
function corrTags(
  parentTable: string,
  csq: CorrelatedSubquery,
  t: Set<string>,
): void {
  if (csq.subquery.table === parentTable) {
    t.add('self_ref');
  }
  if (csq.correlation.parentField.length >= 2) {
    t.add('multi_col_correlation');
  }
}

/** Total EXISTS/NOT-EXISTS nodes anywhere in `ast`. */
function countExists(ast: AST): number {
  const w = ast.where ? countExistsCond(ast.where) : 0;
  return (
    w + (ast.related ?? []).reduce((acc, r) => acc + countExists(r.subquery), 0)
  );
}

function countExistsCond(c: Condition): number {
  switch (c.type) {
    case 'simple':
      return 0;
    case 'correlatedSubquery':
      return 1 + countExists(c.related.subquery);
    case 'and':
    case 'or':
      return c.conditions.reduce((acc, cc) => acc + countExistsCond(cc), 0);
  }
}

/** Max structural nesting depth (root = 0): deepest `related` child or EXISTS subquery. */
function maxDepth(ast: AST): number {
  let m = 0;
  if (ast.where) {
    m = Math.max(m, maxDepthCond(ast.where));
  }
  for (const r of ast.related ?? []) {
    m = Math.max(m, 1 + maxDepth(r.subquery));
  }
  return m;
}

function maxDepthCond(c: Condition): number {
  switch (c.type) {
    case 'simple':
      return 0;
    case 'correlatedSubquery':
      return 1 + maxDepth(c.related.subquery);
    case 'and':
    case 'or':
      return c.conditions.reduce((m, cc) => Math.max(m, maxDepthCond(cc)), 0);
  }
}
