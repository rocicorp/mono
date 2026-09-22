/* oxlint-disable no-console */

/**
 * The **nightly scale subset** of the coverage-driven fuzzer (ported from rusty-ivm
 * `rindle-fuzz/src/main.rs`'s chinook branch): the bounded random tail + swarm run over the
 * **full chinook** dataset under the static cost gate, against the Postgres oracle (z2s).
 *
 * The point is *parity at scale*, not coverage: the generators' literals are tuned to the
 * tiny `mini` fixture, so over the thousands-of-rows chinook tables most filters barely
 * partition — but the engines must still agree, and the deep-nesting tail shapes exercise
 * fan-outs the mini fixture cannot. The cost gate earns its keep here (mini tables are too
 * small to ever trip it).
 *
 * Gated behind `CHINOOK_SCALE=1` (mirroring the Rust nightly's `CHINOOK_SQL` gate): the
 * full-chinook bootstrap + thousands-of-rows hydrates are too heavy for the per-PR lane, so
 * this lane runs nightly / on demand. Deterministic in {@link SEED} (the repro key).
 *
 * Run: `CHINOOK_SCALE=1 pnpm exec vitest run --project='*pg-16*' chinook-fuzz-scale`.
 */

import {expect, test} from 'vitest';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {pkOf} from './fuzz/axes.ts';
import {CostModel} from './fuzz/cost.ts';
import {
  checkSwarm,
  checkTail,
  checkYieldTail,
  panicIfFailed,
} from './fuzz/driver.ts';
import {Data} from './fuzz/literals.ts';
import {miniData} from './fuzz/mini.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';

const RUN = !!process.env.CHINOOK_SCALE;
const SEED = 0x00c0ffee;
const TIMEOUT_MS = 600_000;
const startData = new Data(miniData, pkOf);

/**
 * Approximate full-chinook table sizes (client names) for the static cost gate. Exact
 * counts are unnecessary — the gate is an estimate; these are the published chinook
 * cardinalities.
 */
const CHINOOK_SIZES: ReadonlyArray<readonly [string, number]> = [
  ['artist', 275],
  ['album', 347],
  ['track', 3503],
  ['genre', 25],
  ['mediaType', 5],
  ['playlist', 18],
  ['playlistTrack', 8715],
  ['invoice', 412],
  ['invoiceLine', 2240],
  ['customer', 59],
  ['employee', 8],
];

// Only pay the heavy full-chinook bootstrap when the lane is enabled.
const harness = RUN
  ? await bootstrap({
      suiteName: 'chinook_fuzz_scale',
      zqlSchema: schema,
      pgContent: await getChinook(),
    })
  : null;

if (RUN) {
  console.log(`══ chinook-fuzz scale ══  seed = ${SEED}`);
}

test.skipIf(!RUN)(
  'scale tail — deep random shapes over full chinook (cost-gated)',
  async () => {
    const cost = CostModel.fromSizes(CHINOOK_SIZES, 5_000_000);
    const {report, generated, gated} = await checkTail(
      // oxlint-disable-next-line @typescript-eslint/no-non-null-assertion
      harness!.delegates,
      cost,
      SEED,
      200,
    );
    console.log(
      `chinook tail: generated ${generated} | gated ${gated} | ${report.total} checked | ${report.failures.length} failures`,
    );
    expect(generated).toBeGreaterThan(100);
    expect(report.total).toBeGreaterThan(20); // some run even after the gate trims deep ones
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);

test.skipIf(!RUN)(
  'scale swarm — masked-random over full chinook',
  async () => {
    // oxlint-disable-next-line @typescript-eslint/no-non-null-assertion
    const report = await checkSwarm(harness!.delegates, startData, SEED, 24, 6);
    console.log(
      `chinook swarm: ${report.total} cases, ${report.failures.length} failures`,
    );
    expect(report.total).toBeGreaterThan(40);
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);

test.skipIf(!RUN)(
  'scale random-yield — deep tail hydrate under interleaved sources over full chinook',
  async () => {
    // The reentrancy axis at scale: deep tail shapes hydrated with both IVM sources wrapped
    // so a random fraction of fetch items is preceded by a `'yield'`. Over the thousands-of-
    // rows chinook tables a deep fetch has many yield points, so the engine's cooperative
    // scheduling is stressed on real fan-outs — the heavy interleave the mini per-PR lane
    // only smoke-tests. Hydrate-only (the four-phase push seeds from mini rows). Cost-gated.
    const cost = CostModel.fromSizes(CHINOOK_SIZES, 5_000_000);
    const {report, generated, gated} = await checkYieldTail(
      // oxlint-disable-next-line @typescript-eslint/no-non-null-assertion
      harness!.transact,
      cost,
      SEED ^ 0x1eaf,
      80,
    );
    console.log(
      `chinook yield-tail: generated ${generated} | gated ${gated} | ${report.total} checked | ${report.failures.length} failures`,
    );
    expect(generated).toBeGreaterThan(40);
    expect(report.total).toBeGreaterThan(10);
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);
