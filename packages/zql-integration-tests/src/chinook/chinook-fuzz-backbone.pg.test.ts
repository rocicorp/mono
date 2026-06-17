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
 *   limit × start), lowered onto every decoratable root and onto nested child
 *   collections, hydrates identically — and the realized assignments reach **100%
 *   pairwise** coverage (the design's headline backbone gate).
 *
 * The oracle, the IVM views, and the comparison all come from the existing harness;
 * this file only feeds it the generated cases.
 */

import {expect, test} from 'vitest';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {checkL0Hydrate, checkL1, panicIfFailed} from './fuzz/driver.ts';
import {miniPgContent} from './fuzz/mini.ts';
import {backboneBounds, enumerate} from './fuzz/skeleton.ts';
import {schema} from './schema.ts';

const TIMEOUT_MS = 120_000;

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_backbone',
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
