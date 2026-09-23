/* oxlint-disable no-console */

/**
 * The **per-PR backbone** of the coverage-driven fuzzer (ported from rusty-ivm
 * `rindle-fuzz/tests/backbone.rs`): the cheap, structured layers over the small,
 * self-contained {@link miniPgContent mini} fixture, so CI exercises the small-scope
 * region on every change. The backbone is split across
 * `chinook-fuzz-backbone-*.pg.test.ts` files so CI can spread its lanes over test shards.
 *
 * This file: the **top-N push** lanes, where a `limit` is maintained under pushes.
 *
 * - **Decorated push** — each depth-1 skeleton as a root top-N, every table it touches
 *   driven through the four-phase push protocol, parity-clean at every step.
 * - **Child push** — a `limit` on a nested collection, one window per parent, with the
 *   whole child table drained and refilled.
 *
 * Their heavier variants (depth 2, DESC, the OR shape, every flip plan, forced chunking)
 * run in the nightly, `chinook-fuzz-extended-push.pg.test.ts`.
 */

import {expect, test} from 'vitest';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {pkOf} from './fuzz/axes.ts';
import {
  checkDecoratedPush,
  checkPushCases,
  childPushCases,
  panicIfFailed,
} from './fuzz/driver.ts';
import {Data} from './fuzz/literals.ts';
import {miniData, miniPgContent} from './fuzz/mini.ts';
import {enumerate} from './fuzz/skeleton.ts';
import {schema} from './schema.ts';

const TIMEOUT_MS = 120_000;

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_backbone_decpush',
  zqlSchema: schema,
  pgContent: miniPgContent(),
});

const data = new Data(miniData, pkOf);

// oxlint-disable-next-line expect-expect
test(
  'Decorated push — top-N four-phase per-step parity over mini (D≤1)',
  async () => {
    // The order/limit × push cross-product the other sweeps miss: each depth-1 skeleton
    // becomes a top-N (`orderBy` + small `limit`) and every table it touches is pushed
    // with PER-STEP parity, so a top-N push that strands/drops in-window rows is caught
    // between mutations.
    const skels = enumerate({depth: 1, related: 2, exists: 2});
    const report = await checkDecoratedPush(harness.transact, data, skels, 1);
    console.log(
      `Decorated push backbone (top-N, D≤1): ${report.total} cases, ${report.failures.length} failures`,
    );
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);

test(
  'Child push — a nested top-N window per parent, drained and refilled, per-step parity over mini',
  async () => {
    // Every other push lane limits the root, and the L1 lane only hydrates a limited
    // child. Pairwise covering-array rows that limit the child, each pushed through the
    // whole child table and one row of every other table it touches.
    const cases = childPushCases(data, 2, 1, true);
    expect(cases.length).toBeGreaterThan(0);
    const report = await checkPushCases(harness.transact, cases);
    console.log(
      `Child push backbone (nested top-N): ${report.total} cases, ${report.failures.length} failures`,
    );
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);
