/* oxlint-disable no-console */

/**
 * The **per-PR backbone** of the coverage-driven fuzzer (ported from rusty-ivm
 * `rindle-fuzz/tests/backbone.rs`): the cheap, structured layers over the small,
 * self-contained {@link miniPgContent mini} fixture, so CI exercises the small-scope
 * region on every change. (The full-chinook *scale subset* runs in the nightly,
 * `chinook-fuzz-scale.pg.test.ts`.) The backbone is split across
 * `chinook-fuzz-backbone-*.pg.test.ts` files so CI can spread its lanes over test shards.
 *
 * This file: the **push** lanes. The top-N ones are in
 * `chinook-fuzz-backbone-decpush.pg.test.ts`.
 *
 * - **Push** — every single-level (depth ≤ 1) skeleton, driven through the four-phase
 *   push protocol (membership churn + boundary-crossing edits on every table the query
 *   touches, incl. the EXISTS-gate tables), stays parity-clean at **every** step.
 * - **Pinned push** — the same protocol over correlated-filter shapes.
 * - **Random-yield** — hydrate + push parity while the IVM sources are interleaved
 *   with `'yield'`s.
 *
 * The oracle, the IVM views, and the comparison all come from the existing harness;
 * this file only feeds it the generated cases.
 */

import {expect, test} from 'vitest';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {pkOf} from './fuzz/axes.ts';
import {
  checkPushCases,
  checkPushWalk,
  checkYield,
  checkYieldPush,
  fanInTakeCases,
  l1QueryCases,
  panicIfFailed,
  pinnedPushCases,
} from './fuzz/driver.ts';
import {Data} from './fuzz/literals.ts';
import {miniData, miniPgContent} from './fuzz/mini.ts';
import {fuzzSeed} from './fuzz/seed.ts';
import {enumerate} from './fuzz/skeleton.ts';
import {schema} from './schema.ts';

const TIMEOUT_MS = 120_000;

/** The repro key for the random-yield interleave lanes (`ZERO_FUZZ_SEED`). */
const YIELD_SEED = fuzzSeed();

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_backbone_push',
  zqlSchema: schema,
  pgContent: miniPgContent(),
});

const data = new Data(miniData, pkOf);

// oxlint-disable-next-line expect-expect
test(
  'Push — four-phase per-step parity over mini (D≤1)',
  async () => {
    // Depth ≤ 1: bare roots + single-level relationship/EXISTS fans — where add/remove/
    // edit propagation and gate open-close live.
    const skels = enumerate({depth: 1, related: 2, exists: 2});
    const report = await checkPushWalk(harness.transact, data, skels, 1);
    console.log(
      `Push backbone (D≤1): ${report.total} cases, ${report.failures.length} failures`,
    );
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);

test(
  'Pinned push — a root filter on a join column, every flip plan, four-phase per-step parity over mini (D≤2)',
  async () => {
    // The root pins the join column of its first relationship, which is the shape that
    // correlated predicate pushdown rewrites. D≤2 so the copied filter also sits in the
    // middle of a chain, where a leaf push fetches through it. Every flip assignment runs
    // too, since those are the plans the planner can pick in production.
    const skels = enumerate({depth: 2, related: 1, exists: 1});
    const cases = pinnedPushCases(data, skels, 1);
    expect(cases.filter(c => c.label.includes('|flip')).length).toBeGreaterThan(
      0,
    );
    const report = await checkPushCases(harness.transact, cases);
    console.log(
      `Pinned push backbone (D≤2): ${report.total} cases, ${report.failures.length} failures`,
    );
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);

// oxlint-disable-next-line expect-expect
test(
  'Random-yield — hydrate + push parity under interleaved sources over mini (D≤1)',
  async () => {
    // Both IVM sources (memory + sqlite) are wrapped so a random fraction of fetch/push
    // stream items is preceded by a `'yield'`, perturbing the engine's cooperative
    // scheduling. The PG oracle stays straight, so the interleaved IVM must still match it
    // at every step — the reentrancy axis inherited from the now-removed
    // `chinook-fuzz-hydration` fuzzer. Cheap mini smoke per-PR; the heavy full-chinook
    // interleave rides nightly.
    const skels = enumerate({depth: 1, related: 1, exists: 1});
    const report = await checkYield(
      harness.transact,
      data,
      skels,
      1,
      YIELD_SEED,
    );
    console.log(
      `Random-yield backbone (D≤1): ${report.total} cases, ${report.failures.length} failures`,
    );
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);

test(
  'Random-yield push — Take over UnionFanIn survives interleaved maintenance fetches',
  async () => {
    // The cell no other lane reaches. `checkYield` runs decoration-free skeletons, so it
    // never carries a `limit` (hence never a `Take`); `checkFlipInvariance` enumerates
    // flips but only hydrates. A `Take` sitting above a `UnionFanIn` while a `'yield'`
    // interrupts a maintenance fetch mid-push is a 3-way axis interaction
    // (`flip` x `exists_*_or` x `limit`) that only exists in the corpus at t >= 3.
    const {cases} = l1QueryCases(data);
    const selected = fanInTakeCases(cases);
    expect(
      selected.length,
      'no L1 case builds a Take over a UnionFanIn — the flip axis or t=3 regressed',
    ).toBeGreaterThan(0);
    const report = await checkYieldPush(
      harness.transact,
      data,
      cases,
      1,
      YIELD_SEED,
    );
    console.log(
      `Random-yield push (fan-in+take): ${report.total}/${selected.length} selected, ${report.failures.length} failures`,
    );
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);
