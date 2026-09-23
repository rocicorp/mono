/* oxlint-disable no-console */

/**
 * The **per-PR backbone** of the coverage-driven fuzzer (ported from rusty-ivm
 * `rindle-fuzz/tests/backbone.rs`): the cheap, structured layers over the small,
 * self-contained {@link miniPgContent mini} fixture, so CI exercises the small-scope
 * region on every change. (The full-chinook *scale subset* runs in the nightly,
 * `chinook-fuzz-scale.pg.test.ts`.) The backbone is split across
 * `chinook-fuzz-backbone-*.pg.test.ts` files so CI can spread its lanes over test shards.
 *
 * This file: the **L1** lane, part 1 of 2. The lane's cases are interleaved
 * across `chinook-fuzz-backbone-l1-*.pg.test.ts`, since the whole lane
 * outgrew the test timeout on a loaded CI shard.
 *
 * - **L1** — the pairwise covering array of decorations (filter × exists × order ×
 *   limit × start), lowered onto every decoratable root and onto nested child collections,
 *   hydrates identically — and the realized assignments reach **100% pairwise**
 *   coverage (the design's headline backbone gate).
 *
 * The oracle, the IVM views, and the comparison all come from the existing harness;
 * this file only feeds it the generated cases.
 */

import {expect, test} from 'vitest';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {pkOf} from './fuzz/axes.ts';
import {checkL1, panicIfFailed} from './fuzz/driver.ts';
import {Data} from './fuzz/literals.ts';
import {miniData, miniPgContent} from './fuzz/mini.ts';
import {schema} from './schema.ts';

const PART = 1;
const PARTS = 2;
const TIMEOUT_MS = 120_000;

const harness = await bootstrap({
  suiteName: `chinook_fuzz_backbone_l1_${PART}`,
  zqlSchema: schema,
  pgContent: miniPgContent(),
});

const data = new Data(miniData, pkOf);

test(
  `L1 — 3-way covering array: 100% coverage + hydrate-equal over mini (part ${PART}/${PARTS})`,
  async () => {
    const {report, coverage} = await checkL1(
      harness.delegates,
      data,
      PART,
      PARTS,
    );
    console.log(
      `L1 backbone part ${PART}/${PARTS}: ${report.total} cases, ${coverage.summary()}, ${report.failures.length} failures`,
    );
    expect(
      coverage.fraction(),
      `t-wise coverage incomplete (${coverage.summary()}); missed: ${JSON.stringify(
        coverage.missed(),
      )}`,
    ).toBe(1);
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);
