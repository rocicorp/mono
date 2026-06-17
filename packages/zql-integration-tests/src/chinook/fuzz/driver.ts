/**
 * Orchestration: **layer → differential check → record** (ported from rusty-ivm
 * `rindle-fuzz/src/driver.rs`).
 *
 * A generated query is routed to the differential check ({@link runAndCompare}: the IVM
 * memory + sqlite views vs the Postgres oracle via z2s), every case runs under a caught
 * error so one divergence does not abort the sweep, and all failures are collected and
 * reported **together** with a structural label — a generator regression then surfaces
 * its whole blast radius at once, not just the first case.
 *
 * Unlike the Rust port, the PG oracle + parity comparison are reused wholesale from the
 * existing harness (`helpers/runner.ts`), so this module only owns the layer iteration,
 * failure capture, and coverage accounting. Hydration is read-only, so the whole sweep
 * runs against the shared `delegates` (no per-case transaction needed).
 */

import {astToZQL} from '../../../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../../../ast-to-zql/src/format.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Format} from '../../../../zero-types/src/format.ts';
import {newQueryImpl} from '../../../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {type Delegates, runAndCompare} from '../../helpers/runner.ts';
import {schema} from '../schema.ts';
import {relsOf} from './axes.ts';
import {
  childDecorationPairs,
  decoratableRoots,
  decorate,
  decorateChild,
  greedyCover,
  rowLabel,
} from './cover.ts';
import {Coverage} from './coverage.ts';
import {constructCount, shrinkAst} from './shrink.ts';
import {enumerate, label, lower, type Skeleton} from './skeleton.ts';

/**
 * The maximum number of divergences a single sweep will auto-minimize. Shrinking re-runs
 * the oracle many times per failure, so it is budgeted; the rest are reported un-shrunk
 * (a generator regression that fails everything must not turn into a shrink storm).
 */
const SHRINK_BUDGET = 8;

/** The outcome of a batch of differential checks. */
export type Report = {
  /** How many cases were run. */
  readonly total: number;
  /** `[label, failure message]` for each case that diverged or errored. */
  readonly failures: Array<[string, string]>;
};

/** Run `fn` (a parity assert), returning its error message on failure (truncated). */
export async function capture(fn: () => Promise<void>): Promise<string | null> {
  try {
    await fn();
    return null;
  } catch (e) {
    const msg = e instanceof Error ? (e.stack ?? e.message) : String(e);
    return msg.slice(0, 1500);
  }
}

/**
 * Derive a {@link Format} from an `AST` (singular from relationship cardinality;
 * junction hidden hops collapsed, mirroring the view), so a shrunk AST can be re-wrapped
 * as a runnable query. The format is used identically by the IVM and the oracle, so even
 * an imperfect derivation keeps the differential comparison valid.
 */
function deriveFormat(ast: AST, singular: boolean): Format {
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

/** Whether `ast` (re-wrapped as a query) still diverges from the oracle. */
async function divergesParity(
  delegates: Delegates,
  ast: AST,
): Promise<boolean> {
  const q = newQueryImpl(
    schema,
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    ast.table as any,
    ast,
    deriveFormat(ast, false),
    'test',
  ) as unknown as AnyQuery;
  return (
    (await capture(() => runAndCompare(schema, delegates, q, undefined))) !==
    null
  );
}

/** Shrink a divergent query to a minimal still-divergent repro, rendered as ZQL. */
export async function minimizeRepro(
  delegates: Delegates,
  query: AnyQuery,
): Promise<string> {
  const ast = asQueryInternals(query).ast;
  const minimal = await shrinkAst(ast, a => divergesParity(delegates, a));
  const zql = await formatOutput(minimal.table + astToZQL(minimal));
  return `↓ shrunk to ${constructCount(minimal)} construct(s):\n${zql}`;
}

/**
 * Route one query through hydrate parity (memory + sqlite vs the PG oracle). On a
 * divergence, auto-minimize to a readable repro while `budget` remains (guarded — a
 * shrink failure never breaks the report).
 */
async function checkHydrate(
  delegates: Delegates,
  query: AnyQuery,
  caseLabel: string,
  failures: Array<[string, string]>,
  budget: {remaining: number},
): Promise<void> {
  const msg = await capture(() =>
    runAndCompare(schema, delegates, query, undefined),
  );
  if (!msg) {
    return;
  }
  let full = msg;
  if (budget.remaining > 0) {
    budget.remaining -= 1;
    const repro = await minimizeRepro(delegates, query).catch(() => null);
    if (repro) {
      full = `${repro}\n\n${msg}`;
    }
  }
  failures.push([caseLabel, full]);
}

/** **L0 hydrate sweep:** lower every skeleton and check hydrate parity over the oracle. */
export async function checkL0Hydrate(
  delegates: Delegates,
  skels: readonly Skeleton[],
): Promise<Report> {
  const failures: Array<[string, string]> = [];
  const budget = {remaining: SHRINK_BUDGET};
  for (const s of skels) {
    await checkHydrate(delegates, lower(s), label(s), failures, budget);
  }
  return {total: skels.length, failures};
}

/**
 * **L1 hydrate sweep:** lower the pairwise covering array onto each decoratable root
 * **and** onto nested child collections, check hydrate parity, and accumulate which
 * `(axis, value)` pairwise tuples were realized. Returns the parity {@link Report} and
 * the {@link Coverage} (asserted 100% pairwise by the backbone). A row unrealizable on a
 * target (text filter / no relationship) is skipped there and not counted toward
 * coverage.
 */
export async function checkL1(
  delegates: Delegates,
): Promise<{report: Report; coverage: Coverage}> {
  const rows = greedyCover(2);
  const cov = new Coverage(2);
  const failures: Array<[string, string]> = [];
  const budget = {remaining: SHRINK_BUDGET};
  let total = 0;

  // Root decorations: each covering-array row × each decoratable root.
  for (const row of rows) {
    for (const root of decoratableRoots()) {
      const res = decorate(root, row);
      if (!res) {
        continue;
      }
      total += 1;
      await checkHydrate(
        delegates,
        res[0],
        `L1|${root}|${rowLabel(row)}`,
        failures,
        budget,
      );
      cov.observe(row);
    }
  }

  // Child decorations: each row lowered onto a NESTED collection (per-parent refill,
  // child sort position) — the parity surface the root-only pass misses.
  for (const [parent, rel] of childDecorationPairs()) {
    for (const row of rows) {
      const res = decorateChild(parent, rel, row);
      if (!res) {
        continue;
      }
      total += 1;
      await checkHydrate(
        delegates,
        res[0],
        `L1|${parent}.${rel}|${rowLabel(row)}`,
        failures,
        budget,
      );
      cov.observe(row);
    }
  }

  return {report: {total, failures}, coverage: cov};
}

/**
 * Throw with a collected summary (all failing labels + the first `nShow` diffs) if
 * anything failed. A no-op when the batch was clean.
 */
export function panicIfFailed(report: Report, nShow: number): void {
  if (report.failures.length === 0) {
    return;
  }
  const labels = report.failures.map(([l]) => l).join('\n  ');
  const shown = report.failures
    .slice(0, nShow)
    .map(([l, m]) => `CASE ${l}\n${m}`)
    .join('\n\n========================================\n\n');
  const extra = report.failures.length - nShow;
  const more =
    extra > 0 ? `\n\n... and ${extra} more (see the label list above)` : '';
  throw new Error(
    `${report.failures.length}/${report.total} generated cases failed.\n\n` +
      `ALL FAILING LABELS:\n  ${labels}\n\n` +
      `FIRST ${nShow} DIFFS:\n\n${shown}${more}`,
  );
}

/** Re-export for the test entry to enumerate the backbone. */
export {enumerate};
