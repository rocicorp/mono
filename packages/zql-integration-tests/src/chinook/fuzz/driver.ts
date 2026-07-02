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
  type Source,
} from '../../../../zql/src/ivm/source.ts';
import {consume} from '../../../../zql/src/ivm/stream.ts';
import {createRandomYieldWrapper} from '../../../../zql/src/ivm/test/random-yield-source.ts';
import {asQueryInternals} from '../../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {mapResultToClientNames} from '../../../../zqlite/src/test/source-factory.ts';
import {type Delegates, runAndCompare} from '../../helpers/runner.ts';
import {schema} from '../schema.ts';
import type {CostModel} from './cost.ts';
import {
  applyLimit,
  applyOrder,
  childDecorationPairs,
  decoratableRoots,
  decorate,
  decorateChild,
  greedyCover,
  rowLabel,
} from './cover.ts';
import {Coverage} from './coverage.ts';
import {flipAssignments, flippableExistsCount, setFlips} from './flip.ts';
import type {Data} from './literals.ts';
import {mutate} from './mutate.ts';
import {type Mutation, pushForSkeleton} from './push.ts';
import type {Regression} from './regressions.ts';
import {rng} from './rng.ts';
import {
  buildScalar,
  hasScalarSubquery,
  resolveScalarForIvm,
  scalarCandidates,
} from './scalar.ts';
import {constructCount, shrinkAst} from './shrink.ts';
import {enumerate, label, lower, type Skeleton} from './skeleton.ts';
import {Mask, swarmGen} from './swarm.ts';
import {type DeepBounds, tailBounds, tailGen} from './tail.ts';
import {wrapAst} from './wrap.ts';

/** A delegate transaction-scoping function (from `bootstrap().transact`). */
export type Transact = (
  cb: (delegates: Delegates) => Promise<void>,
  /** Optionally wrap each IVM source (memory + sqlite) — used by the random-yield sweep. */
  sourceWrapper?: (source: Source) => Source,
) => Promise<void>;

/**
 * The maximum number of divergences a single sweep will auto-minimize. Shrinking re-runs
 * the oracle many times per failure, so it is budgeted; the rest are reported un-shrunk
 * (a generator regression that fails everything must not turn into a shrink storm).
 */
const SHRINK_BUDGET = 8;

/**
 * The per-yield-point probability the random-yield interleave sweep injects a `'yield'`
 * marker into a source fetch/push stream (matches the old random-only hydration fuzzer,
 * `chinook-fuzz-hydration`, which this axis replaced).
 */
const YIELD_P = 0.3;

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
  data: Data,
): Promise<{report: Report; coverage: Coverage}> {
  const rows = greedyCover(2);
  const cov = new Coverage(2);
  const failures: Array<[string, string]> = [];
  const budget = {remaining: SHRINK_BUDGET};
  let total = 0;

  // Root decorations: each covering-array row × each decoratable root.
  for (const row of rows) {
    for (const root of decoratableRoots()) {
      const res = decorate(root, row, data);
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
      const res = decorateChild(parent, rel, row, data);
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
  data: Data,
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
      const res = swarmGen(r, mask, data);
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

// ── flip-invariance (plan-choice invariance of EXISTS gates) ──────────────────────────

/**
 * **Flip-invariance sweep:** for each EXISTS-bearing skeleton, lower it and hydrate it
 * under **every** `2^k` flip assignment of its `k` positive EXISTS gates (semi-join vs
 * `FlippedJoin`), checking each against the Postgres oracle. Since `flip` is a plan choice
 * the oracle ignores, every assignment must agree with the oracle — hence with each other.
 * Skeletons with no flippable gate (none / only NOT-EXISTS) are skipped. `k` is capped so
 * the `2^k` fan-out stays bounded.
 */
export async function checkFlipInvariance(
  delegates: Delegates,
  skels: readonly Skeleton[],
  maxFlips = 4,
): Promise<Report> {
  const failures: Array<[string, string]> = [];
  const budget = {remaining: SHRINK_BUDGET};
  let total = 0;
  for (const s of skels) {
    const base = asQueryInternals(lower(s)).ast;
    const k = flippableExistsCount(base);
    if (k === 0 || k > maxFlips) {
      continue;
    }
    for (const bits of flipAssignments(k)) {
      total += 1;
      await checkHydrate(
        delegates,
        wrapAst(setFlips(base, bits)),
        `flip|${label(s)}|${bits.map(b => (b ? 1 : 0)).join('')}`,
        failures,
        budget,
      );
    }
  }
  return {total, failures};
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
 * **Decorated-push sweep:** push over **top-N** queries — the cross-product the rest of
 * the fuzzer leaves uncovered. Elsewhere `order`/`limit` are hydrate-only (L1, swarm,
 * tail) and the push sweep carries no decorations, so a `whereExists(...).orderBy().limit()`
 * **push** is never exercised. Here each skeleton is lowered, given a root `orderBy` + a
 * small `limit`, and pushed (root + EXISTS-gated leaf) with parity re-checked **after every
 * mutation** — a top-N push that strands or drops an in-window row is wrong *between*
 * mutations even when a later mutation restores the seed, a transient a final-state
 * comparison misses.
 */
export async function checkDecoratedPush(
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
    const topN = applyLimit(applyOrder(lower(s), s.table, 'asc1'), 'small');
    const msg = await capture(() =>
      transact(d => pushWalk(d, topN, mutations)),
    );
    if (msg) {
      failures.push([`decpush|${label(s)}`, msg]);
    }
  }
  return {total, failures};
}

/**
 * **Random-yield interleave sweep** (ported from the now-removed `chinook-fuzz-hydration`
 * fuzzer's `createRandomYieldWrapper` axis): re-run hydrate + four-phase push parity for each
 * skeleton with **both** IVM sources (memory + sqlite) wrapped in a `RandomYieldSource`
 * that injects `'yield'` markers at random fetch/push points (probability {@link YIELD_P}).
 *
 * This perturbs the IVM's cooperative scheduling — exercising the operators' yield handling
 * during a live fetch *and* a live push, the reentrancy/interleaving failure class the rest
 * of the fuzzer (which always pulls the sources straight) never probes. The PG oracle is not
 * wrapped: it stays the ground truth the interleaved IVM must still match.
 *
 * Each skeleton runs under its own yield stream seeded from `(seed, index)`, so a divergence
 * replays bit-for-bit. A skeleton with push history runs the four-phase walk (whose initial
 * compare is the hydrate-under-yield check); a mutation-free one runs a plain hydrate.
 */
export async function checkYield(
  transact: Transact,
  data: Data,
  skels: readonly Skeleton[],
  n: number,
  seed: number,
): Promise<Report> {
  const failures: Array<[string, string]> = [];
  let total = 0;
  let idx = 0;
  for (const s of skels) {
    const i = idx++;
    // Per-skeleton deterministic yield stream so a failure replays from (seed, index).
    const r = rng((seed ^ Math.imul(i + 1, 0x9e3779b9)) >>> 0);
    const wrap = createRandomYieldWrapper(() => r.float(), YIELD_P);
    const query = lower(s);
    const mutations = pushForSkeleton(data, s, n);
    total += 1;
    const msg = await capture(() =>
      transact(
        d =>
          mutations.length > 0
            ? pushWalk(d, query, mutations)
            : runAndCompare(schema, d, query, undefined),
        wrap,
      ),
    );
    if (msg) {
      failures.push([`yield|${label(s)}`, msg]);
    }
  }
  return {total, failures};
}

/**
 * **Random-yield hydrate sweep (generator-fed):** generate `n` random deep tail queries from
 * `seed`, skip the cost-gated ones, and hydrate each under **yield-wrapped** sources against
 * the oracle. The scale-lane analogue of {@link checkYield}: over the large chinook fixture
 * the deep fetches have many yield points, so the interleave is stressed on real fan-outs.
 * Push is mini-fixture-only (the four-phase seed rows are mini rows), so this scale path is
 * hydrate-only. Deterministic in `seed` — a divergence replays from `(seed, index)`.
 */
export async function checkYieldTail(
  transact: Transact,
  cost: CostModel,
  seed: number,
  n: number,
  bounds: DeepBounds = tailBounds(),
): Promise<TailReport> {
  const r = rng(seed);
  const failures: Array<[string, string]> = [];
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
    // A fresh per-case yield stream so a divergence replays from (seed, index).
    const yr = rng((seed ^ Math.imul(i + 1, 0x85ebca6b)) >>> 0);
    const wrap = createRandomYieldWrapper(() => yr.float(), YIELD_P);
    const msg = await capture(() =>
      transact(d => runAndCompare(schema, d, res[0], undefined), wrap),
    );
    if (msg) {
      failures.push([`yieldtail|seed${seed}|${i}`, msg]);
    }
  }
  return {report: {total, failures}, generated, gated};
}

/**
 * **Scalar-subquery sweep:** for every one-hop relationship, build a PK-constrained (hence
 * *simple*) scalar subquery and check that the production split agrees with the oracle — the
 * original `scalar: true` AST through z2s (`parentField = (SELECT childField … LIMIT 1)`)
 * vs the IVM over the **pre-resolved** AST (`parentField = <literal>`, the transform
 * zero-cache's pipeline-driver applies via {@link resolveSimpleScalarSubqueries}).
 *
 * `rawRows` (client-named) backs the synchronous scalar executor; it must match the data
 * loaded into the oracle so both sides pick the same row. A candidate whose subquery fails
 * to resolve (would leave the base IVM treating `scalar` as a plain EXISTS — a generation
 * regression, not an engine bug) is reported, not silently passed.
 */
export async function checkScalar(
  delegates: Delegates,
  rawRows: Record<string, readonly Row[]>,
  data: Data,
): Promise<Report> {
  const failures: Array<[string, string]> = [];
  let total = 0;
  for (const cand of scalarCandidates()) {
    const q = buildScalar(cand, data);
    if (!q) {
      continue; // empty child table — no present PK to constrain
    }
    total += 1;
    const lbl = `scalar|${cand.table}.${cand.rel}`;
    const msg = await capture(async () => {
      const ast = asQueryInternals(q).ast;
      const resolved = resolveScalarForIvm(ast, rawRows);
      if (hasScalarSubquery(resolved)) {
        throw new Error(
          `scalar subquery did not resolve (would diverge spuriously) for ${lbl}`,
        );
      }
      const resolvedQuery = wrapAst(resolved);
      // Oracle runs the ORIGINAL scalar AST (z2s); the IVM runs the RESOLVED one.
      const pgResult = await delegates.pg.run(q);
      const sqliteResult = mapResultToClientNames(
        await delegates.sqlite.run(resolvedQuery),
        schema,
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
        ast.table as any,
      );
      const memoryResult = await delegates.memory.run(resolvedQuery);
      expect(memoryResult).toEqualPg(pgResult);
      expect(sqliteResult).toEqualPg(pgResult);
    });
    if (msg) {
      failures.push([lbl, msg]);
    }
  }
  return {total, failures};
}

/**
 * **Corpus-first regression replay** (design §9): re-run every committed regression through
 * the differential check it was filed under — hydrate parity, or per-step push parity when
 * it carries a push history — so each past find is a permanent guard. A no-op when no
 * regressions are committed yet.
 */
export async function checkRegressions(
  delegates: Delegates,
  transact: Transact,
  regs: readonly Regression[],
): Promise<Report> {
  const failures: Array<[string, string]> = [];
  for (const reg of regs) {
    const hasPush = (reg.pushes?.length ?? 0) > 0;
    const msg = hasPush
      ? await capture(() =>
          transact(d => pushWalk(d, wrapAst(reg.ast), reg.pushes ?? [])),
        )
      : await capture(() =>
          runAndCompare(schema, delegates, wrapAst(reg.ast), undefined),
        );
    if (msg) {
      failures.push([`regress|${reg.note}`, msg]);
    }
  }
  return {total: regs.length, failures};
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
