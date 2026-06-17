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

import {expect} from 'vitest';
import {astToZQL} from '../../../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../../../ast-to-zql/src/format.ts';
import {must} from '../../../../shared/src/must.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {NameMapper} from '../../../../zero-schema/src/name-mapper.ts';
import {makeServerTransaction} from '../../../../zero-server/src/custom.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../../../../zql/src/ivm/source.ts';
import {consume} from '../../../../zql/src/ivm/stream.ts';
import {asQueryInternals} from '../../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {mapResultToClientNames} from '../../../../zqlite/src/test/source-factory.ts';
import {type Delegates, runAndCompare} from '../../helpers/runner.ts';
import {schema} from '../schema.ts';
import type {CostModel} from './cost.ts';
import {
  childDecorationPairs,
  decoratableRoots,
  decorate,
  decorateChild,
  greedyCover,
  rowLabel,
} from './cover.ts';
import {Coverage} from './coverage.ts';
import type {Data} from './literals.ts';
import {mutate} from './mutate.ts';
import {type Mutation, pushForSkeleton} from './push.ts';
import {rng} from './rng.ts';
import {constructCount, shrinkAst} from './shrink.ts';
import {enumerate, label, lower, type Skeleton} from './skeleton.ts';
import {Mask, swarmGen} from './swarm.ts';
import {type DeepBounds, tailBounds, tailGen} from './tail.ts';
import {wrapAst} from './wrap.ts';

/** A delegate transaction-scoping function (from `bootstrap().transact`). */
export type Transact = (
  cb: (delegates: Delegates) => Promise<void>,
) => Promise<void>;

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

/** Whether `ast` (re-wrapped as a query) still diverges from the oracle. */
async function divergesParity(
  delegates: Delegates,
  ast: AST,
): Promise<boolean> {
  return (
    (await capture(() =>
      runAndCompare(schema, delegates, wrapAst(ast), undefined),
    )) !== null
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

// ── the randomized layers (L2 swarm / L3 mutation / L4 random tail) ────────────────────

/**
 * **L2 swarm sweep:** draw `nMasks` random feature masks from `seed`, generate `perMask`
 * masked-random queries per mask, and check hydrate parity. Bugs that surface only when a
 * feature is *absent* live here. Deterministic in `seed` (printed on failure — the repro
 * key).
 */
export async function checkSwarm(
  delegates: Delegates,
  seed: number,
  nMasks: number,
  perMask: number,
): Promise<Report> {
  const r = rng(seed);
  const failures: Array<[string, string]> = [];
  const budget = {remaining: SHRINK_BUDGET};
  let total = 0;
  for (let mi = 0; mi < nMasks; mi++) {
    const mask = Mask.random(r);
    for (let qi = 0; qi < perMask; qi++) {
      const res = swarmGen(r, mask);
      if (!res) {
        continue;
      }
      total += 1;
      await checkHydrate(
        delegates,
        res[0],
        `swarm|seed${seed}|m${mi}q${qi}`,
        failures,
        budget,
      );
    }
  }
  return {total, failures};
}

/**
 * **L3 mutation sweep:** apply one random mutation to each corpus skeleton ("simple + one
 * twist"), checking hydrate parity. Each base is `lower(skeleton)`'s AST; the mutated AST
 * is re-wrapped and run. Deterministic in `seed`.
 */
export async function checkMutate(
  delegates: Delegates,
  corpus: readonly Skeleton[],
  seed: number,
): Promise<Report> {
  const r = rng(seed);
  const failures: Array<[string, string]> = [];
  const budget = {remaining: SHRINK_BUDGET};
  for (const s of corpus) {
    const baseAst = asQueryInternals(lower(s)).ast;
    const mutated = mutate(r, baseAst);
    await checkHydrate(
      delegates,
      wrapAst(mutated),
      `mutate|${label(s)}`,
      failures,
      budget,
    );
  }
  return {total: corpus.length, failures};
}

/** The L4 random-tail outcome — the gated (too-expensive, skipped) count reported, not
 * silently dropped. */
export type TailReport = {
  readonly report: Report;
  /** How many queries the generator produced (before the cost gate). */
  readonly generated: number;
  /** How many the static cost gate rejected (skipped, never run). */
  readonly gated: number;
};

/**
 * **L4 random-tail sweep:** generate `n` random deep queries from `seed`, skip + count the
 * ones the static {@link CostModel} gate rejects, run the rest through hydrate parity, and
 * collect any divergences. Deterministic in `seed`.
 */
export async function checkTail(
  delegates: Delegates,
  cost: CostModel,
  seed: number,
  n: number,
  bounds: DeepBounds = tailBounds(),
): Promise<TailReport> {
  const r = rng(seed);
  const failures: Array<[string, string]> = [];
  const budget = {remaining: SHRINK_BUDGET};
  let generated = 0;
  let gated = 0;
  let total = 0;
  for (let i = 0; i < n; i++) {
    const res = tailGen(r, bounds);
    if (!res) {
      continue;
    }
    generated += 1;
    const ast = asQueryInternals(res[0]).ast;
    if (cost.tooExpensive(ast)) {
      gated += 1; // static gate: skip + count, never run
      continue;
    }
    total += 1;
    await checkHydrate(
      delegates,
      res[0],
      `tail|seed${seed}|${i}`,
      failures,
      budget,
    );
  }
  return {report: {total, failures}, generated, gated};
}

// ── the four-phase push protocol (per-step parity) ────────────────────────────────────

function mapRow(row: Row, table: string, mapper: NameMapper): Row {
  const out: Record<string, Row[string]> = {};
  for (const [col, value] of Object.entries(row)) {
    out[mapper.columnName(table, col)] = value;
  }
  return out;
}

/** Apply one mutation to the Postgres oracle + both IVM sources (memory + sqlite). */
async function applyMutation(
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  serverTx: any,
  delegates: Delegates,
  m: Mutation,
): Promise<void> {
  const memSrc = must(delegates.memory.getSource(m.table));
  const sqlSrc = must(
    delegates.sqlite.getSource(delegates.mapper.tableName(m.table)),
  );
  switch (m.kind) {
    case 'remove':
      await serverTx.mutate[m.table].delete(m.row);
      consume(
        sqlSrc.push(
          makeSourceChangeRemove(mapRow(m.row, m.table, delegates.mapper)),
        ),
      );
      consume(memSrc.push(makeSourceChangeRemove(m.row)));
      break;
    case 'add':
      await serverTx.mutate[m.table].insert(m.row);
      consume(
        sqlSrc.push(
          makeSourceChangeAdd(mapRow(m.row, m.table, delegates.mapper)),
        ),
      );
      consume(memSrc.push(makeSourceChangeAdd(m.row)));
      break;
    case 'edit':
      await serverTx.mutate[m.table].update(m.row);
      consume(
        sqlSrc.push(
          makeSourceChangeEdit(
            mapRow(m.row, m.table, delegates.mapper),
            mapRow(m.old, m.table, delegates.mapper),
          ),
        ),
      );
      consume(memSrc.push(makeSourceChangeEdit(m.row, m.old)));
      break;
  }
}

/**
 * Materialize the IVM memory + sqlite views once, then apply `mutations` one at a time,
 * re-checking parity against the (recomputed) oracle **after every step** — catching an
 * accumulation drift or a transient wrong state a single end-of-batch comparison would
 * mask. Throws (a parity assertion) on the first divergence.
 */
async function pushWalk(
  delegates: Delegates,
  query: AnyQuery,
  mutations: readonly Mutation[],
): Promise<void> {
  const table = asQueryInternals(query).ast.table;
  const memView = delegates.memory.materialize(query);
  const sqliteView = delegates.sqlite.materialize(query);
  const serverTx = await makeServerTransaction(
    delegates.pg.transaction,
    'test-client',
    0,
    schema,
  );
  const compare = async () => {
    const pg = await delegates.pg.run(query);
    expect(
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      mapResultToClientNames(sqliteView.data, schema, table as any),
    ).toEqualPg(pg);
    expect(memView.data).toEqualPg(pg);
  };
  try {
    await compare(); // initial (hydration) state
    for (const m of mutations) {
      await applyMutation(serverTx, delegates, m);
      await compare();
    }
  } finally {
    memView.destroy();
    sqliteView.destroy();
  }
}

/**
 * **Push sweep:** lower each skeleton, generate its four-phase push history (root +
 * deepest leaf), and check per-step push parity inside a rolled-back transaction. `n`
 * rows per mutated table. The four-phase sequence is net-zero (it restores the seed), so
 * a clean skeleton leaves the data pristine for the next.
 */
export async function checkPushWalk(
  transact: Transact,
  data: Data,
  skels: readonly Skeleton[],
  n: number,
): Promise<Report> {
  const failures: Array<[string, string]> = [];
  let total = 0;
  for (const s of skels) {
    const mutations = pushForSkeleton(data, s, n);
    if (mutations.length === 0) {
      continue;
    }
    total += 1;
    const msg = await capture(() =>
      transact(d => pushWalk(d, lower(s), mutations)),
    );
    if (msg) {
      failures.push([`push|${label(s)}`, msg]);
    }
  }
  return {total, failures};
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
