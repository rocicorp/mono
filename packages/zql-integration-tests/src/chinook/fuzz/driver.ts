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
import type {
  AST,
  LiteralValue,
  SimpleOperator,
} from '../../../../zero-protocol/src/ast.ts';
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
import {
  axisIndex,
  LIMIT_VALS,
  pinOn,
  relOf,
  relPath,
  type OrderVal,
} from './axes.ts';
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
import {
  flipAssignments,
  flippableExistsCount,
  flipVariants,
  setFlips,
} from './flip.ts';
import type {Data} from './literals.ts';
import {mutate} from './mutate.ts';
import {
  fourPhase,
  type Mutation,
  pushForChild,
  pushForQuery,
  queryTables,
} from './push.ts';
import type {Regression} from './regressions.ts';
import {rng} from './rng.ts';
import {scalarizableExistsCount, setScalars} from './scalar.ts';
import {reproHint} from './seed.ts';
import {constructCount, shrinkAst} from './shrink.ts';
import {enumerate, label, lower, lowerOr, type Skeleton} from './skeleton.ts';
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

/** One generated hydrate/query case, independent of the execution target. */
export type QueryCase = {
  readonly label: string;
  readonly query: AnyQuery;
};

/** One generated mutation-maintenance case, independent of the execution target. */
export type PushCase = QueryCase & {
  readonly mutations: readonly Mutation[];
};

/** A target-specific checker for a generated query case. */
export type QueryCaseChecker = (
  query: AnyQuery,
  label: string,
) => Promise<void>;

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
 * Run generated query cases through a target-provided checker. This is the shared
 * concentric-ring harness: the corpus is generated once, and each ring supplies only
 * the layer-specific comparison implementation.
 */
export async function checkQueryCases(
  cases: readonly QueryCase[],
  check: QueryCaseChecker,
): Promise<Report> {
  const failures: Array<[string, string]> = [];
  for (const c of cases) {
    const msg = await capture(() => check(c.query, c.label));
    if (msg) {
      failures.push([c.label, msg]);
    }
  }
  return {total: cases.length, failures};
}

/** The L0 structural skeleton corpus as target-independent query cases. */
export function skeletonQueryCases(
  skels: readonly Skeleton[],
): readonly QueryCase[] {
  return skels.map(s => ({label: label(s), query: lower(s)}));
}

/**
 * The L1 decoration corpus as target-independent query cases.
 *
 * Strength defaults to **3**. Pairwise is not enough once `flip` is an axis: the shapes
 * that matter are 3-way — a flipped gate must sit *inside an OR* (that is what makes
 * `builder.ts` construct the `UnionFanOut`/`UnionFanIn` pair) *and* carry a `limit` (to
 * put a `Take` above the fan-in). At `t=2` the greedy cover realizes
 * `flip x exists_or x limit` in zero rows; at `t=3`, in 19.
 */
export function l1QueryCases(
  data: Data,
  t = 3,
): {
  readonly cases: readonly QueryCase[];
  readonly coverage: Coverage;
} {
  const rows = greedyCover(t);
  const coverage = new Coverage(t);
  const cases: QueryCase[] = [];

  for (const row of rows) {
    for (const root of decoratableRoots()) {
      const res = decorate(root, row, data);
      if (!res) {
        continue;
      }
      cases.push({
        label: `L1|${root}|${rowLabel(row)}`,
        query: res[0],
      });
      coverage.observe(row);
    }
  }

  for (const [parent, rel] of childDecorationPairs()) {
    for (const row of rows) {
      const res = decorateChild(parent, rel, row, data);
      if (!res) {
        continue;
      }
      cases.push({
        label: `L1|${parent}.${rel}|${rowLabel(row)}`,
        query: res[0],
      });
      coverage.observe(row);
    }
  }

  return {cases, coverage};
}

/** The L2 swarm corpus as target-independent query cases. */
export function swarmQueryCases(
  data: Data,
  seed: number,
  nMasks: number,
  perMask: number,
): readonly QueryCase[] {
  const r = rng(seed);
  const cases: QueryCase[] = [];
  for (let mi = 0; mi < nMasks; mi++) {
    const mask = Mask.random(r);
    for (let qi = 0; qi < perMask; qi++) {
      const res = swarmGen(r, mask, data);
      if (!res) {
        continue;
      }
      cases.push({
        label: `swarm|seed${seed}|m${mi}q${qi}`,
        query: res[0],
      });
    }
  }
  return cases;
}

/** The L3 mutation-from-corpus cases as target-independent query cases. */
export function mutationQueryCases(
  corpus: readonly Skeleton[],
  seed: number,
): readonly QueryCase[] {
  const r = rng(seed);
  return corpus.map(s => {
    const baseAst = asQueryInternals(lower(s)).ast;
    const mutated = mutate(r, baseAst);
    return {label: `mutate|${label(s)}`, query: wrapAst(mutated)};
  });
}

/** The L4 random-tail generated cases after cost gating. */
export type TailCases = {
  readonly cases: readonly QueryCase[];
  readonly generated: number;
  readonly gated: number;
};

export function tailQueryCases(
  cost: CostModel,
  seed: number,
  n: number,
  bounds: DeepBounds = tailBounds(),
): TailCases {
  const r = rng(seed);
  const cases: QueryCase[] = [];
  let generated = 0;
  let gated = 0;
  for (let i = 0; i < n; i++) {
    const res = tailGen(r, bounds);
    if (!res) {
      continue;
    }
    generated += 1;
    const ast = asQueryInternals(res[0]).ast;
    if (cost.tooExpensive(ast)) {
      gated += 1;
      continue;
    }
    cases.push({label: `tail|seed${seed}|${i}`, query: res[0]});
  }
  return {cases, generated, gated};
}

/** Flip-invariance variants as target-independent query cases. */
export function flipQueryCases(
  skels: readonly Skeleton[],
  maxFlips = 4,
): readonly QueryCase[] {
  const cases: QueryCase[] = [];
  for (const s of skels) {
    const base = asQueryInternals(lower(s)).ast;
    const k = flippableExistsCount(base);
    if (k === 0 || k > maxFlips) {
      continue;
    }
    for (const bits of flipAssignments(k)) {
      cases.push({
        label: `flip|${label(s)}|${bits.map(b => (b ? 1 : 0)).join('')}`,
        query: wrapAst(setFlips(base, bits)),
      });
    }
  }
  return cases;
}

/** Scalar-invariance variants as target-independent query cases. */
export function scalarQueryCases(
  skels: readonly Skeleton[],
  maxScalars = 4,
): readonly QueryCase[] {
  const cases: QueryCase[] = [];
  for (const s of skels) {
    const base = asQueryInternals(lower(s)).ast;
    const k = scalarizableExistsCount(base);
    if (k === 0 || k > maxScalars) {
      continue;
    }
    // The same `{false, true}^k` enumeration flip-invariance uses.
    for (const bits of flipAssignments(k)) {
      cases.push({
        label: `scalar|${label(s)}|${bits.map(b => (b ? 1 : 0)).join('')}`,
        query: wrapAst(setScalars(base, bits)),
      });
    }
  }
  return cases;
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
  return await checkHydrateCases(delegates, skeletonQueryCases(skels));
}

async function checkHydrateCases(
  delegates: Delegates,
  cases: readonly QueryCase[],
): Promise<Report> {
  const failures: Array<[string, string]> = [];
  const budget = {remaining: SHRINK_BUDGET};
  for (const c of cases) {
    await checkHydrate(delegates, c.query, c.label, failures, budget);
  }
  return {total: cases.length, failures};
}

/**
 * **L1 hydrate sweep:** lower the pairwise covering array onto each decoratable root
 * **and** onto nested child collections, check hydrate parity, and accumulate which
 * `(axis, value)` pairwise tuples were realized. Returns the parity {@link Report} and
 * the {@link Coverage} (asserted 100% pairwise by the backbone). A row unrealizable on a
 * target (text filter / no relationship) is skipped there and not counted toward
 * coverage.
 *
 * `part` of `parts` runs only every `parts`-th case (starting at `part - 1`), so the
 * sweep can be split across test files. The split interleaves rather than slicing
 * contiguous ranges because the corpus lists the cheap root decorations before the far
 * slower nested-child ones. The returned {@link Coverage} is always that of the whole
 * corpus.
 */
export async function checkL1(
  delegates: Delegates,
  data: Data,
  part = 1,
  parts = 1,
): Promise<{report: Report; coverage: Coverage}> {
  const {cases, coverage} = l1QueryCases(data);
  const slice = cases.filter((_, i) => i % parts === part - 1);
  return {report: await checkHydrateCases(delegates, slice), coverage};
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
  return await checkHydrateCases(
    delegates,
    swarmQueryCases(data, seed, nMasks, perMask),
  );
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
  return await checkHydrateCases(delegates, mutationQueryCases(corpus, seed));
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
  const {cases, generated, gated} = tailQueryCases(cost, seed, n, bounds);
  return {
    report: await checkHydrateCases(delegates, cases),
    generated,
    gated,
  };
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
  return await checkHydrateCases(delegates, flipQueryCases(skels, maxFlips));
}

// ── the four-phase push protocol (per-step parity) ────────────────────────────────────

function mapRow(row: Row, table: string, mapper: NameMapper): Row {
  const out: Record<string, Row[string]> = {};
  for (const [col, value] of Object.entries(row)) {
    out[mapper.columnName(table, col)] = value;
  }
  return out;
}

/**
 * Apply one mutation to the Postgres oracle + every IVM source (memory, sqlite, and
 * deferred-write sqlite when the delegates have it).
 */
async function applyMutation(
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  serverTx: any,
  delegates: Delegates,
  m: Mutation,
): Promise<void> {
  const {mapper} = delegates;
  const memSrc = must(delegates.memory.getSource(m.table));
  const sqlSrcs = [delegates.sqlite, delegates.sqliteDeferred]
    .filter(d => d !== undefined)
    .map(d => must(d.getSource(mapper.tableName(m.table))));
  const row = mapRow(m.row, m.table, mapper);
  switch (m.kind) {
    case 'remove':
      await serverTx.mutate[m.table].delete(m.row);
      for (const src of sqlSrcs) {
        consume(src.push(makeSourceChangeRemove(row)));
      }
      consume(memSrc.push(makeSourceChangeRemove(m.row)));
      break;
    case 'add':
      await serverTx.mutate[m.table].insert(m.row);
      for (const src of sqlSrcs) {
        consume(src.push(makeSourceChangeAdd(row)));
      }
      consume(memSrc.push(makeSourceChangeAdd(m.row)));
      break;
    case 'edit': {
      await serverTx.mutate[m.table].update(m.row);
      const old = mapRow(m.old, m.table, mapper);
      for (const src of sqlSrcs) {
        consume(src.push(makeSourceChangeEdit(row, old)));
      }
      consume(memSrc.push(makeSourceChangeEdit(m.row, m.old)));
      break;
    }
  }
}

/**
 * Materialize the IVM memory + sqlite views once, then apply `mutations` one at a time,
 * re-checking parity against the (recomputed) oracle **after every step** — catching an
 * accumulation drift or a transient wrong state a single end-of-batch comparison would
 * mask. Throws (a parity assertion) on the first divergence.
 *
 * When the delegates have a deferred-write sqlite delegate (every `transact` does), its
 * view is walked and checked too: its sources hold the mutations in memory and merge
 * them into every fetch, which is how zero-cache derives with `deferIvmWrites`.
 */
async function pushWalk(
  delegates: Delegates,
  query: AnyQuery,
  mutations: readonly Mutation[],
): Promise<void> {
  const table = asQueryInternals(query).ast.table;
  const memView = delegates.memory.materialize(query);
  const sqliteView = delegates.sqlite.materialize(query);
  const deferredView = delegates.sqliteDeferred?.materialize(query);
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
    if (deferredView) {
      expect(
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
        mapResultToClientNames(deferredView.data, schema, table as any),
        'deferred-write sqlite view',
      ).toEqualPg(pg);
    }
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
    deferredView?.destroy();
  }
}

/**
 * Push-maintenance cases generated from skeletons, independent of the target. Every
 * table the query touches is mutated ({@link pushForQuery}): mutating only the root and
 * the deepest leaf never pushed to a skeleton's other children, or to a junction table.
 */
export function pushCases(
  data: Data,
  skels: readonly Skeleton[],
  n: number,
): readonly PushCase[] {
  const cases: PushCase[] = [];
  for (const s of skels) {
    const query = lower(s);
    const mutations = pushForQuery(data, s, asQueryInternals(query).ast, n);
    if (mutations.length === 0) {
      continue;
    }
    cases.push({label: `push|${label(s)}`, query, mutations});
  }
  return cases;
}

/** Which top-N cases {@link decoratedPushCases} builds from each skeleton. */
export type DecoratedPushOptions = {
  /** The root `orderBy` under the `limit` (default `asc1`). */
  readonly order?: OrderVal | undefined;
  /**
   * Also lower a skeleton with root gates as those gates ORed with a simple root filter
   * ({@link lowerOr}), which puts a fan-in under the `Take` (default false).
   */
  readonly or?: boolean | undefined;
  /**
   * Also run every flip assignment of up to this many gates, since a flipped gate is the
   * only thing that builds a `UnionFanIn` (default 0: the builder's plan only).
   */
  readonly maxFlips?: number | undefined;
};

/**
 * Top-N push-maintenance cases generated from skeletons, independent of the target: each
 * lowering gets a root `orderBy` + small `limit`, and every table the query touches is
 * mutated ({@link pushForQuery}), so one source change can reach the limited window
 * through two connections (a self-join, two paths to one table).
 */
export function decoratedPushCases(
  data: Data,
  skels: readonly Skeleton[],
  n: number,
  {order = 'asc1', or = false, maxFlips = 0}: DecoratedPushOptions = {},
): readonly PushCase[] {
  const cases: PushCase[] = [];
  for (const s of skels) {
    const shapes: Array<[string, AnyQuery]> = [['and', lower(s)]];
    if (or && s.children.some(c => c.kind !== 'related')) {
      shapes.push(['or', lowerOr(s)]);
    }
    for (const [shape, lowered] of shapes) {
      const base = applyLimit(applyOrder(lowered, s.table, order), 'small');
      const mutations = pushForQuery(data, s, asQueryInternals(base).ast, n);
      if (mutations.length === 0) {
        continue;
      }
      for (const [suffix, query] of queryFlipVariants(base, maxFlips)) {
        cases.push({
          label: `decpush|${order}|${shape}|${label(s)}${suffix}`,
          query,
          mutations,
        });
      }
    }
  }
  return cases;
}

/**
 * Push cases for a **limited nested collection** (ported from rindle's
 * `check_decorated_push_child`): each covering-array row is lowered onto the child of a
 * `(parent, relationship)` pair ({@link decorateChild}) and driven through
 * {@link pushForChild}'s history.
 *
 * {@link decoratedPushCases} only ever puts the `limit` at the root, and the L1 lane only
 * *hydrates* a decorated child. A nested `limit` or `start` builds one window per parent,
 * each with its own state, refill and eviction, and a hydrate rebuilds them all from the
 * source, so only a push can observe one that went wrong: rindle's version of a window
 * that drained to empty and then dropped every later add for its parent was invisible to
 * both of those lanes. Draining the whole child table takes every window to empty, and
 * adding it back refills them.
 *
 * `windowedOnly` keeps just the rows that decorate the child with a `limit`, the ones that
 * build a partitioned `Take`.
 */
export function childPushCases(
  data: Data,
  t: number,
  n: number,
  windowedOnly: boolean,
): readonly PushCase[] {
  const rows = greedyCover(t);
  const noLimit = LIMIT_VALS.indexOf('none');
  const cases: PushCase[] = [];
  for (const [parent, rel] of childDecorationPairs()) {
    const path = relPath(parent, rel);
    for (const row of rows) {
      if (windowedOnly && row[axisIndex('limit')] === noLimit) {
        continue;
      }
      const res = decorateChild(parent, rel, row, data);
      if (!res) {
        continue;
      }
      const ast = asQueryInternals(res[0]).ast;
      cases.push({
        label: `childpush|${parent}.${rel}|${rowLabel(row)}`,
        query: res[0],
        mutations: pushForChild(data, path, ast, n),
      });
    }
  }
  return cases;
}

/**
 * Push cases whose root `where` pins the join column of the root's first relationship,
 * with `=` on one present value or `IN` on two. Correlated predicate pushdown copies such
 * a pin into the child, and on down a chain that correlates on the same column, so these
 * are the queries it rewrites. Every table in the query is mutated, so pushes cross the
 * copied filter both from the parent side and from the child side.
 *
 * Each pinned query is also run under every flip assignment of its EXISTS gates (up to
 * `maxFlips` gates). In production the planner can flip a pinned EXISTS child, and a
 * flipped child is where the copied pin matters most: it is the only thing that limits
 * the outer loop's read of the child.
 */
export function pinnedPushCases(
  data: Data,
  skels: readonly Skeleton[],
  n: number,
  maxFlips = 4,
): readonly PushCase[] {
  const cases: PushCase[] = [];
  for (const s of skels) {
    if (s.children.length === 0) {
      continue;
    }
    const pin = pinOn(
      s.table,
      must(relOf(s.table, s.children[0].rel)).parentField[0],
    );
    if (!pin) {
      continue;
    }
    const base = lower(s);
    const mutations = [...queryTables(asQueryInternals(base).ast)].flatMap(t =>
      fourPhase(data, t, n),
    );
    const pins: Array<[string, SimpleOperator, LiteralValue]> = [
      ['eq', '=', pin.eq],
      ['in', 'IN', pin.in],
    ];
    for (const [tag, op, value] of pins) {
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      const pinned: AnyQuery = (base as any).where(pin.col, op, value);
      for (const [suffix, query] of queryFlipVariants(pinned, maxFlips)) {
        cases.push({
          label: `pinpush|${tag}|${label(s)}${suffix}`,
          query,
          mutations,
        });
      }
    }
  }
  return cases;
}

/**
 * Check per-step push parity for each case inside a rolled-back transaction, collecting
 * every failure.
 */
export async function checkPushCases(
  transact: Transact,
  cases: readonly PushCase[],
): Promise<Report> {
  const failures: Array<[string, string]> = [];
  for (const c of cases) {
    const msg = await capture(() =>
      transact(d => pushWalk(d, c.query, c.mutations)),
    );
    if (msg) {
      failures.push([c.label, msg]);
    }
  }
  return {total: cases.length, failures};
}

/**
 * **Push sweep:** lower each skeleton, generate its four-phase push history (every table
 * the query touches), and check per-step push parity inside a rolled-back transaction. `n`
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
  const cases = pushCases(data, skels, n);
  for (const c of cases) {
    const msg = await capture(() =>
      transact(d => pushWalk(d, c.query, c.mutations)),
    );
    if (msg) {
      failures.push([c.label, msg]);
    }
  }
  return {total: cases.length, failures};
}

/**
 * **Decorated-push sweep:** push over **top-N** queries — the cross-product the rest of
 * the fuzzer leaves uncovered. Elsewhere `order`/`limit` are hydrate-only (L1, swarm,
 * tail) and the push sweep carries no decorations, so a `whereExists(...).orderBy().limit()`
 * **push** is never exercised. Here each skeleton is lowered, given a root `orderBy` + a
 * small `limit`, and pushed (every table it touches) with parity re-checked **after every
 * mutation** — a top-N push that strands or drops an in-window row is wrong *between*
 * mutations even when a later mutation restores the seed, a transient a final-state
 * comparison misses. `opts` adds the OR shape and flip plans ({@link DecoratedPushOptions}).
 */
export async function checkDecoratedPush(
  transact: Transact,
  data: Data,
  skels: readonly Skeleton[],
  n: number,
  opts: DecoratedPushOptions = {},
): Promise<Report> {
  const failures: Array<[string, string]> = [];
  const cases = decoratedPushCases(data, skels, n, opts);
  for (const c of cases) {
    const msg = await capture(() =>
      transact(d => pushWalk(d, c.query, c.mutations)),
    );
    if (msg) {
      failures.push([c.label, msg]);
    }
  }
  return {total: cases.length, failures};
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
  maxFlips = 2,
): Promise<Report> {
  const failures: Array<[string, string]> = [];
  let total = 0;
  let idx = 0;
  for (const s of skels) {
    const mutations = pushForQuery(data, s, asQueryInternals(lower(s)).ast, n);
    for (const [suffix, query] of yieldPlanVariants(s, maxFlips)) {
      const i = idx++;
      // Per-variant deterministic yield stream so a failure replays from (seed, index).
      const r = rng((seed ^ Math.imul(i + 1, 0x9e3779b9)) >>> 0);
      const wrap = createRandomYieldWrapper(() => r.float(), YIELD_P);
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
        failures.push([`yield|${label(s)}${suffix}`, msg]);
      }
    }
  }
  return {total, failures};
}

/**
 * The plan variants a skeleton contributes to the yield lane: the builder's default
 * lowering, plus **every** flip assignment of its positive EXISTS gates.
 *
 * Flips are not cosmetic here. A flipped gate is the only thing that makes `builder.ts`
 * construct a `UnionFanOut`/`UnionFanIn` pair, so without them the yield lane never
 * interleaves a fetch or push through a fan-in at all — the operator whose maintenance
 * fetch is the one that has to survive a `'yield'`. The flip-invariance lane enumerates
 * the same assignments but only *hydrates* them; this is where they meet pushes.
 */
function yieldPlanVariants(
  s: Skeleton,
  maxFlips: number,
): Array<[string, AnyQuery]> {
  return queryFlipVariants(lower(s), maxFlips);
}

/**
 * `query` as lowered, plus **every** other flip assignment of its positive EXISTS gates
 * (only `query` when it has none, or more than `maxFlips`). The planner only ever changes
 * flips, so these are all the plans it can produce for `query`.
 */
function queryFlipVariants(
  query: AnyQuery,
  maxFlips: number,
): Array<[string, AnyQuery]> {
  const ast = asQueryInternals(query).ast;
  return [
    ['', query],
    ...flipVariants(ast, maxFlips).map(
      ([suffix, flippedAst]): [string, AnyQuery] => [
        suffix,
        wrapAst(flippedAst),
      ],
    ),
  ];
}

/**
 * The L1 cases that build a **`Take` above a `UnionFanIn`**: a flipped EXISTS gate (the
 * only thing that makes `builder.ts` construct a `UnionFanOut`/`UnionFanIn` pair) sitting
 * under a `limit`. This is a 3-way axis interaction (`flip` x `exists_*_or` x `limit`), so
 * it exists in the corpus only at `t >= 3`.
 */
export function fanInTakeCases(
  cases: readonly QueryCase[],
): readonly QueryCase[] {
  return cases.filter(c => {
    const ast = asQueryInternals(c.query).ast;
    return (
      ast.limit !== undefined &&
      JSON.stringify(ast.where ?? null).includes('"flip":true')
    );
  });
}

/**
 * **Random-yield push sweep over decorated L1 cases.**
 *
 * {@link checkYield} runs *skeletons* — structure only, no decorations — so it never has a
 * `limit`, hence never a `Take`. {@link checkFlipInvariance} enumerates flips but only
 * hydrates. Neither lane can reach a `Take` sitting above a `UnionFanIn` while a `'yield'`
 * interrupts a maintenance fetch mid-push, which is exactly where that pair breaks.
 *
 * This lane closes that cell: decorated cases (so `limit` is real), filtered to the ones
 * that actually build the fan-in, driven through the four-phase push walk with both IVM
 * sources yield-wrapped. `max` caps the fan-out so it stays a per-PR cost.
 */
export async function checkYieldPush(
  transact: Transact,
  data: Data,
  cases: readonly QueryCase[],
  n: number,
  seed: number,
  max = 48,
): Promise<Report> {
  const selected = fanInTakeCases(cases).slice(0, max);
  const failures: Array<[string, string]> = [];
  let total = 0;
  for (let i = 0; i < selected.length; i++) {
    const c = selected[i];
    const r = rng((seed ^ Math.imul(i + 1, 0x27d4eb2f)) >>> 0);
    const wrap = createRandomYieldWrapper(() => r.float(), YIELD_P);
    const ast = asQueryInternals(c.query).ast;
    // Mutate both sides of the gate: parent pushes drive the fan-out, child pushes drive
    // the flipped join's own push path into the fan-in.
    const mutations = [...queryTables(ast)].flatMap(t => fourPhase(data, t, n));
    if (mutations.length === 0) {
      continue;
    }
    total += 1;
    const msg = await capture(() =>
      transact(d => pushWalk(d, c.query, mutations), wrap),
    );
    if (msg) {
      failures.push([`yieldPush|${c.label}`, msg]);
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
 * **Scalar-invariance sweep:** `scalar` is a plan hint every engine in the differential is
 * free to ignore, so all `2^k` scalar assignments of an EXISTS-bearing skeleton must agree
 * with the oracle — hence with each other. Unlike the old lane, gates are *not* pinned to a
 * unique key first: an undecorated skeleton gate is unpinned, which is exactly the space
 * where z2s used to decorrelate unsoundly.
 */
export async function checkScalarInvariance(
  delegates: Delegates,
  skels: readonly Skeleton[],
  maxScalars = 4,
): Promise<Report> {
  return await checkHydrateCases(
    delegates,
    scalarQueryCases(skels, maxScalars),
  );
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
 * Throw with a collected summary (all failing labels + the first `nShow` diffs, and the
 * seed to replay with if the file read one) if anything failed. A no-op when the batch
 * was clean.
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
      `FIRST ${nShow} DIFFS:\n\n${shown}${more}${reproHint()}`,
  );
}

/** Re-export for the test entry to enumerate the backbone. */
export {enumerate};
