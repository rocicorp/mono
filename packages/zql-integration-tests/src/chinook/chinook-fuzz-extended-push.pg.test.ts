/* oxlint-disable no-console */

/**
 * The **nightly** top-N push lanes (ported from rusty-ivm `rindle-fuzz/tests/backbone.rs`'s
 * `nested_decorated_push_flip_baseline`): the per-PR lanes of
 * `chinook-fuzz-backbone-decpush.pg.test.ts` widened along the axes that are too heavy to
 * pay for on every pull request.
 *
 * - **depth 2** — a gate nested under a gate, so a bottom-level change runs a maintenance
 *   fetch through two joins while the outer one is mid-push.
 * - **the OR shape and every flip plan** — a flipped gate inside an OR is what builds a
 *   `UnionFanOut`/`UnionFanIn` under the `Take`.
 * - **DESC** — a reverse scan takes different bound and refill paths.
 * - **forced chunking** — `FlippedJoin` splits a batch of parent keys into chunks of 256
 *   and merges the chunks' streams. Mini never has more than a handful of keys, so only a
 *   chunk size of 2 reaches the merge.
 * - **the 3-way covering array** for the nested-window lane, over every row it can realize
 *   and two rows of each non-path table.
 *
 * Runs only when `ZERO_FUZZ_BUDGET` is above 1 (the nightly; see `fuzz/seed.ts`).
 */

import {expect, test} from 'vitest';
import {setMultiConstraintChunkSizeForTest} from '../../../zql/src/ivm/flipped-join.ts';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {pkOf} from './fuzz/axes.ts';
import {
  checkDecoratedPush,
  checkPushCases,
  childPushCases,
  decoratedPushCases,
  panicIfFailed,
  type Report,
} from './fuzz/driver.ts';
import {Data} from './fuzz/literals.ts';
import {miniData, miniPgContent} from './fuzz/mini.ts';
import {fuzzBudget} from './fuzz/seed.ts';
import {enumerate} from './fuzz/skeleton.ts';
import {schema} from './schema.ts';

const RUN = fuzzBudget() > 1;
const TIMEOUT_MS = 900_000;

/** The production chunk size (`undefined`), and one small enough to force the merge. */
const CHUNKS = [undefined, 2] as const;

/**
 * Cases that diverged when these lanes were added, at every chunk size. A lane fails on
 * any other divergence, and on any of these that stops diverging, so the list only
 * shrinks.
 *
 * The `childpush` ones fail on the memory source too, and predate #6620. Minimized:
 * `mediaType.related('tracks', t => t.whereExists('album').orderBy('milliseconds').limit(2))`,
 * then remove albums 10 and 11 and add them back; adding 11 sends a child collection a
 * remove for a row it does not hold (`node does not exist`).
 *
 * The `decpush` ones all go through `invoiceLine.customer`, whose hidden middle hop is
 * `invoice`, and fail on the SQLite source only. Minimized, both kinds are: delete and
 * re-insert an `invoiceLine` row, then remove (and re-add) its `invoice`, under a `limit`.
 *
 * - `asc1`: the view is sent a remove for a row it does not hold (`node does not
 *   exist`). The minimized case also fails before #6620.
 * - `desc1`: the removed invoice's track stays in the top-N. The minimized case is a
 *   regression from #6620 (two-phase `Take` refills), and zero-cache's view-syncer
 *   keeps the stale row on the client too.
 */
const KNOWN_FAILURES: ReadonlySet<string> = new Set([
  'decpush|asc1|and|track(rel:album,nx:invoiceLine(ex:customer))',
  'decpush|asc1|and|track(rel:album,nx:invoiceLine(ex:customer))|flip10',
  'decpush|asc1|and|track(rel:genre,nx:invoiceLine(ex:customer))',
  'decpush|asc1|and|track(rel:genre,nx:invoiceLine(ex:customer))|flip10',
  'decpush|asc1|and|track(rel:mediaType,nx:invoiceLine(ex:customer))',
  'decpush|asc1|and|track(rel:mediaType,nx:invoiceLine(ex:customer))|flip10',
  'decpush|asc1|and|track(rel:playlist,nx:invoiceLine(ex:customer))',
  'decpush|asc1|and|track(rel:playlist,nx:invoiceLine(ex:customer))|flip10',
  'decpush|asc1|and|track(rel:playlistTrack,nx:invoiceLine(ex:customer))',
  'decpush|asc1|and|track(rel:playlistTrack,nx:invoiceLine(ex:customer))|flip10',
  'decpush|asc1|and|track(nx:invoiceLine(ex:customer))',
  'decpush|asc1|and|track(nx:invoiceLine(ex:customer))|flip10',
  'decpush|desc1|and|track(rel:album,ex:invoiceLine(ex:customer))',
  'decpush|desc1|and|track(rel:album,ex:invoiceLine(nx:customer))',
  'decpush|desc1|and|track(rel:album,ex:invoiceLine(nx:customer))|flip10',
  'decpush|desc1|and|track(rel:genre,ex:invoiceLine(ex:customer))',
  'decpush|desc1|and|track(rel:genre,ex:invoiceLine(nx:customer))',
  'decpush|desc1|and|track(rel:genre,ex:invoiceLine(nx:customer))|flip10',
  'decpush|desc1|and|track(rel:mediaType,ex:invoiceLine(ex:customer))',
  'decpush|desc1|and|track(rel:mediaType,ex:invoiceLine(nx:customer))',
  'decpush|desc1|and|track(rel:mediaType,ex:invoiceLine(nx:customer))|flip10',
  'decpush|desc1|and|track(rel:playlist,ex:invoiceLine(ex:customer))',
  'decpush|desc1|and|track(rel:playlist,ex:invoiceLine(nx:customer))',
  'decpush|desc1|and|track(rel:playlist,ex:invoiceLine(nx:customer))|flip10',
  'decpush|desc1|and|track(rel:playlistTrack,ex:invoiceLine(ex:customer))',
  'decpush|desc1|and|track(rel:playlistTrack,ex:invoiceLine(nx:customer))',
  'decpush|desc1|and|track(rel:playlistTrack,ex:invoiceLine(nx:customer))|flip10',
  'decpush|desc1|and|track(ex:invoiceLine(ex:customer))',
  'decpush|desc1|and|track(ex:invoiceLine(nx:customer))',
  'decpush|desc1|and|track(ex:invoiceLine(nx:customer))|flip10',
  'childpush|mediaType.tracks|filter=like exists=exists_and order=desc1 limit=small start=none flip=none',
  'childpush|mediaType.tracks|filter=like exists=exists_or order=desc1 limit=small start=none flip=none',
  'childpush|mediaType.tracks|filter=ilike exists=exists_and order=desc1 limit=small start=none flip=none',
  'childpush|mediaType.tracks|filter=ilike exists=exists_or order=desc1 limit=small start=none flip=none',
  'childpush|mediaType.tracks|filter=and2 exists=exists_and order=desc1 limit=small start=none flip=none',
  'childpush|mediaType.tracks|filter=and2 exists=exists_or order=desc1 limit=small start=none flip=none',
]);

/**
 * Fail on a divergence not in {@link KNOWN_FAILURES}, or on a known one among `labels`
 * that no longer diverges.
 */
function checkAgainstKnown(report: Report, labels: readonly string[]): void {
  const failed = new Set(report.failures.map(([label]) => label));
  panicIfFailed(
    {
      total: report.total,
      failures: report.failures.filter(([label]) => !KNOWN_FAILURES.has(label)),
    },
    12,
  );
  const fixed = labels.filter(l => KNOWN_FAILURES.has(l) && !failed.has(l));
  expect(fixed, 'now passing: remove them from KNOWN_FAILURES').toEqual([]);
}

// Only pay for the bootstrap when the lanes run.
const harness = RUN
  ? await bootstrap({
      suiteName: 'chinook_fuzz_extended_push',
      zqlSchema: schema,
      pgContent: miniPgContent(),
    })
  : null;

const data = new Data(miniData, pkOf);

/** Run `check` with `FlippedJoin`'s chunk size forced to `chunk` (if set). */
async function withChunk(
  chunk: number | undefined,
  check: () => Promise<Report>,
): Promise<Report> {
  const restore =
    chunk === undefined ? undefined : setMultiConstraintChunkSizeForTest(chunk);
  try {
    return await check();
  } finally {
    restore?.();
  }
}

for (const order of ['asc1', 'desc1'] as const) {
  for (const chunk of CHUNKS) {
    test.skipIf(!RUN)(
      `Decorated push — D≤2, AND + OR, every flip plan, ${order}, chunk ${chunk ?? 'default'}`,
      async () => {
        const skels = enumerate({depth: 2, related: 1, exists: 2});
        const opts = {order, or: true, maxFlips: 2};
        const report = await withChunk(chunk, () =>
          checkDecoratedPush(
            // oxlint-disable-next-line @typescript-eslint/no-non-null-assertion
            harness!.transact,
            data,
            skels,
            1,
            opts,
          ),
        );
        console.log(
          `Extended decorated push (D≤2, ${order}, chunk ${chunk ?? 'default'}): ${report.total} cases, ${report.failures.length} failures`,
        );
        expect(report.total).toBeGreaterThan(0);
        checkAgainstKnown(
          report,
          decoratedPushCases(data, skels, 1, opts).map(c => c.label),
        );
      },
      TIMEOUT_MS,
    );
  }
}

for (const chunk of CHUNKS) {
  test.skipIf(!RUN)(
    `Child push — every 3-way row, chunk ${chunk ?? 'default'}`,
    async () => {
      const cases = childPushCases(data, 3, 2, false);
      expect(cases.length).toBeGreaterThan(0);
      const report = await withChunk(chunk, () =>
        // oxlint-disable-next-line @typescript-eslint/no-non-null-assertion
        checkPushCases(harness!.transact, cases),
      );
      console.log(
        `Extended child push (3-way, chunk ${chunk ?? 'default'}): ${report.total} cases, ${report.failures.length} failures`,
      );
      checkAgainstKnown(
        report,
        cases.map(c => c.label),
      );
    },
    TIMEOUT_MS,
  );
}
