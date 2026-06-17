/* oxlint-disable no-console */

/**
 * The **per-PR backbone** of the coverage-driven fuzzer (ported from rusty-ivm
 * `rindle-fuzz/tests/backbone.rs`): the cheap, structured layers over the small,
 * self-contained {@link miniPgContent mini} fixture, so CI exercises the small-scope
 * region on every change. (The full-chinook *scale subset* runs nightly — a later
 * phase.)
 *
 * - **L0** — every bounded-exhaustive skeleton (depth ≤ 2) hydrates identically through
 *   the IVM memory + sqlite views and the Postgres oracle (z2s).
 * - **L1** — the pairwise covering array of decorations (filter × exists × order ×
 *   limit), lowered onto every decoratable root and onto nested child collections,
 *   hydrates identically — and the realized assignments reach **100% pairwise** coverage
 *   (the design's headline backbone gate).
 * - **Push** — every single-level (depth ≤ 1) skeleton, driven through the four-phase
 *   push protocol (root + leaf membership churn + boundary-crossing edits, incl. the
 *   EXISTS-gate table), stays parity-clean at **every** step.
 *
 * The oracle, the IVM views, and the comparison all come from the existing harness;
 * this file only feeds it the generated cases.
 */

import {expect, test} from 'vitest';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {pkOf} from './fuzz/axes.ts';
import {
  checkDecoratedPush,
  checkFlipInvariance,
  checkL0Hydrate,
  checkL1,
  checkPushWalk,
  panicIfFailed,
} from './fuzz/driver.ts';
import {Data} from './fuzz/literals.ts';
import {miniData, miniPgContent} from './fuzz/mini.ts';
import {backboneBounds, enumerate} from './fuzz/skeleton.ts';
import {schema} from './schema.ts';

const TIMEOUT_MS = 120_000;

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_backbone',
  zqlSchema: schema,
  pgContent: miniPgContent(),
});

const data = new Data(miniData, pkOf);

// oxlint-disable-next-line expect-expect
test(
  'L0 — bounded-exhaustive skeletons hydrate-equal over mini',
  async () => {
    const skels = enumerate(backboneBounds());
    const report = await checkL0Hydrate(harness.delegates, skels);
    console.log(
      `L0 backbone (D≤2): ${report.total} skeletons, ${report.failures.length} failures`,
    );
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);

test(
  'L1 — pairwise covering array: 100% coverage + hydrate-equal over mini',
  async () => {
    const {report, coverage} = await checkL1(harness.delegates);
    console.log(
      `L1 backbone: ${report.total} cases, ${coverage.summary()}, ${report.failures.length} failures`,
    );
    expect(
      coverage.fraction(),
      `pairwise coverage incomplete (${coverage.summary()}); missed: ${JSON.stringify(
        coverage.missed(),
      )}`,
    ).toBe(1);
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);

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

// oxlint-disable-next-line expect-expect
test(
  'Flip-invariance — every flip plan of an EXISTS query hydrate-equal over mini',
  async () => {
    // `flip` is a plan choice (semi-join vs FlippedJoin) the IVM honors and z2s ignores,
    // so every 2^k flip assignment of an EXISTS-bearing skeleton must agree with the
    // oracle — hence with each other. D≤1 (root single/double gates) is the cheap per-PR
    // surface; deeper flip×flip nesting rides the nightly sweep.
    const skels = enumerate({depth: 1, related: 1, exists: 2});
    const report = await checkFlipInvariance(harness.delegates, skels);
    console.log(
      `Flip backbone: ${report.total} flip-variants, ${report.failures.length} failures`,
    );
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);

// oxlint-disable-next-line expect-expect
test(
  'Decorated push — top-N four-phase per-step parity over mini (D≤1)',
  async () => {
    // The order/limit × push cross-product the other sweeps miss: each depth-1 skeleton
    // becomes a top-N (`orderBy` + small `limit`) and is pushed (incl. the EXISTS-gated
    // leaf) with PER-STEP parity, so a top-N push that strands/drops in-window rows is
    // caught between mutations.
    const skels = enumerate({depth: 1, related: 2, exists: 2});
    const report = await checkDecoratedPush(harness.transact, data, skels, 1);
    console.log(
      `Decorated push backbone (top-N, D≤1): ${report.total} cases, ${report.failures.length} failures`,
    );
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);
