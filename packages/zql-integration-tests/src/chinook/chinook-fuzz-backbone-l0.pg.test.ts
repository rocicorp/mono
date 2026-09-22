/* oxlint-disable no-console */

/**
 * The **per-PR backbone** of the coverage-driven fuzzer (ported from rusty-ivm
 * `rindle-fuzz/tests/backbone.rs`): the cheap, structured layers over the small,
 * self-contained {@link miniPgContent mini} fixture, so CI exercises the small-scope
 * region on every change. (The full-chinook *scale subset* runs nightly — a later
 * phase.) The backbone is split across `chinook-fuzz-backbone-*.pg.test.ts` files so
 * CI can spread its lanes over test shards.
 *
 * This file: the **L0** and **flip-invariance** lanes.
 *
 * - **L0** — every bounded-exhaustive skeleton (depth ≤ 2) hydrates identically through
 *   the IVM memory + sqlite views and the Postgres oracle (z2s).
 * - **Flip-invariance** — every flip plan of an EXISTS-bearing skeleton hydrates
 *   identically, since `flip` is a plan choice the oracle ignores.
 *
 * The oracle, the IVM views, and the comparison all come from the existing harness;
 * this file only feeds it the generated cases.
 */

import {test} from 'vitest';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {
  checkFlipInvariance,
  checkL0Hydrate,
  panicIfFailed,
} from './fuzz/driver.ts';
import {miniPgContent} from './fuzz/mini.ts';
import {backboneBounds, enumerate} from './fuzz/skeleton.ts';
import {schema} from './schema.ts';

const TIMEOUT_MS = 120_000;

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_backbone_l0',
  zqlSchema: schema,
  pgContent: miniPgContent(),
});

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
