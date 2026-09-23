/* oxlint-disable no-console */

/**
 * The **randomized sweep** of the coverage-driven fuzzer (ported from rusty-ivm
 * `rindle-fuzz/src/main.rs`'s `run_full_stack`): the seeded, feedback-light layers that
 * cover the long tail the bounded backbone can't reach, all over the small, self-contained
 * {@link miniPgContent mini} fixture and checked against the Postgres oracle (z2s).
 *
 * - **L2 swarm** — per random feature mask, a handful of masked-random queries; bugs that
 *   surface only when a feature is *absent* live here.
 * - **L3 mutation** — every bounded skeleton + one random twist ("simple + one twist",
 *   where minimal repros live).
 * - **L4 random tail** — random deep shapes (depth ≤ 4) the enumerator can't reach, under
 *   the static cost gate.
 *
 * Every layer is **deterministic in {@link SEED}** (printed up front, embedded in each
 * case label and in the failure summary — the repro key): a divergence replays
 * bit-for-bit. Any failure is auto-minimized to a readable repro by the driver's shrinker.
 *
 * Per-PR this is the cheap mini-scale randomized net that runs alongside the structured
 * backbone, on the fixed default seed. The nightly (`.github/workflows/fuzz-nightly.yml`)
 * re-runs it on a fresh `ZERO_FUZZ_SEED` with the case counts scaled by
 * `ZERO_FUZZ_BUDGET` (see `fuzz/seed.ts`), next to the full-chinook scale subset.
 */

import {expect, test} from 'vitest';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {pkOf} from './fuzz/axes.ts';
import {CostModel} from './fuzz/cost.ts';
import {
  checkMutate,
  checkSwarm,
  checkTail,
  panicIfFailed,
} from './fuzz/driver.ts';
import {Data} from './fuzz/literals.ts';
import {miniData, miniPgContent} from './fuzz/mini.ts';
import {formatSeed, fuzzBudget, fuzzSeed} from './fuzz/seed.ts';
import {enumerate} from './fuzz/skeleton.ts';
import {schema} from './schema.ts';

/** The repro key (design §9): `ZERO_FUZZ_SEED`, or the fixed per-PR default. */
const SEED = fuzzSeed();
/** Case-count multiplier: 1 per-PR, larger nightly. */
const BUDGET = fuzzBudget();
const TIMEOUT_MS = 120_000 * BUDGET;
const data = new Data(miniData, pkOf);

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_sweep',
  zqlSchema: schema,
  pgContent: miniPgContent(),
});

console.log(
  `══ chinook-fuzz sweep ══  seed = ${formatSeed(SEED)}  budget = ${BUDGET}`,
);

test(
  'L2 swarm — masked-random queries hydrate-equal over mini',
  async () => {
    const report = await checkSwarm(
      harness.delegates,
      data,
      SEED,
      16 * BUDGET,
      4,
    );
    console.log(
      `swarm: ${report.total} cases, ${report.failures.length} failures`,
    );
    // Most masked picks are realizable; a regression that emits ~no cases fails here.
    expect(report.total).toBeGreaterThan(30);
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);

test(
  'L3 mutation — corpus + one twist hydrate-equal over mini',
  async () => {
    // The corpus: bounded skeletons (a depth-2 root + one related + one exists), each
    // given one random mutation. Capped so the per-PR sweep stays cheap.
    const corpus = enumerate({depth: 2, related: 1, exists: 1}).slice(
      0,
      100 * BUDGET,
    );
    const report = await checkMutate(harness.delegates, corpus, SEED ^ 0x5eed);
    console.log(
      `mutate: ${report.total} cases, ${report.failures.length} failures`,
    );
    expect(report.total).toBe(corpus.length);
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);

test(
  'L4 random tail — deep random shapes hydrate-equal over mini (cost-gated)',
  async () => {
    // Over mini every shape is cheap, so the gate rarely fires; it is exercised in earnest
    // by the nightly chinook scale subset. Generous threshold mirrors the Rust mini sweep.
    const cost = CostModel.fromData(miniData, 1_000_000);
    const {report, generated, gated} = await checkTail(
      harness.delegates,
      cost,
      SEED,
      150 * BUDGET,
    );
    console.log(
      `tail: generated ${generated} | gated ${gated} | ${report.total} checked | ${report.failures.length} failures`,
    );
    expect(generated).toBeGreaterThan(100);
    expect(report.total).toBeGreaterThan(50);
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);
