import {beforeAll, describe, expect, test} from 'vitest';
import {must} from '../../../shared/src/must.ts';
import {
  createPlannerInfrastructure,
  type PlanAttemptResult,
} from '../helpers/planner-exec.ts';
import {runAndCompare} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';

/**
 * End-to-end validation of NOT EXISTS (anti-join) planning against the real
 * Chinook dataset, measured in actual rows scanned during hydration.
 *
 * NOT EXISTS joins can never be flipped, so the planner's job for a query
 * containing one is to:
 * 1. plan it at all (a NOT EXISTS-only query has exactly one plan, which
 *    still needs constraints propagated and a cost estimate), and
 * 2. estimate its cost/selectivity well enough that flip decisions for
 *    sibling EXISTS joins are correct.
 *
 * "Unplanned" baselines are produced by pinning the sibling EXISTS with
 * `{flip: false}` (the query exactly as written) and the alternative with
 * `{flip: true}`, then comparing the planner's pick against both.
 *
 * Useful Chinook facts (fixed dataset, so these are stable):
 * - 3503 tracks; 1519 of them appear in no invoiceLine ("unsold").
 * - Exactly one album titled 'Frank' (id 322). Its tracks are at the very
 *   END of the track table (ids 3467+) and exactly 5 of them are unsold.
 * - Exactly one album titled 'Big Ones'. Its tracks are near the START of
 *   the track table.
 * - 71 of 275 artists have no albums; 4 of 18 playlists have no tracks.
 */

const pgContent = await getChinook();

const infra = await createPlannerInfrastructure({
  suiteName: 'chinook_planner_not_exists',
  pgContent,
  schema,
  // The "indexed" database mimics a production deployment: filter columns
  // and FKs used by these queries are indexed.
  indices: [
    'CREATE INDEX IF NOT EXISTS idx_album_title ON album(title)',
    'CREATE INDEX IF NOT EXISTS idx_track_album_id ON track(album_id)',
    'CREATE INDEX IF NOT EXISTS idx_invoice_line_track_id ON invoice_line(track_id)',
  ],
});

const {queries, delegates, executeAllPlanAttempts} = infra;

function picked(results: PlanAttemptResult[]): PlanAttemptResult {
  return results.reduce((best, r) =>
    r.estimatedCost < best.estimatedCost ? r : best,
  );
}

function baseline(results: PlanAttemptResult[]): PlanAttemptResult {
  return must(
    results.find(r => r.attemptNumber === 0),
    'baseline (attempt 0) missing',
  );
}

function optimal(results: PlanAttemptResult[]): PlanAttemptResult {
  return results.reduce((best, r) =>
    r.actualRowsScanned < best.actualRowsScanned ? r : best,
  );
}

// Query under test: "the first 5 unsold tracks on the album 'Frank'".
// The 5 matching tracks are the last rows of the track table, so the
// as-written plan scans essentially the whole table.
function unsoldFrankTracks(flip?: boolean) {
  return queries.track
    .where(({not, exists}) => not(exists('invoiceLines')))
    .whereExists(
      'album',
      a => a.where('title', 'Frank'),
      flip === undefined ? undefined : {flip},
    )
    .orderBy('id', 'asc')
    .limit(5);
}

describe('Chinook NOT EXISTS planning', {timeout: 60_000}, () => {
  beforeAll(() => {
    infra.initializePlannerInfrastructure();
    infra.initializeIndexedDatabase();
  });

  test('a NOT EXISTS-only query is planned (exactly one attempt)', () => {
    // No join in this query is flippable, but the planner must still
    // evaluate the single all-semi plan so the query gets constraint
    // propagation and cost estimates (e.g. for analyze-query output).
    const query = queries.artist.where(({not, exists}) =>
      not(exists('albums')),
    );

    const results = executeAllPlanAttempts(query);
    expect(results).toHaveLength(1);
    expect(results[0].flipPattern).toBe(0);
    expect(results[0].actualRowsScanned).toBeGreaterThan(0);
  });

  test('pinning flip on the sibling EXISTS yields exactly the forced plan', () => {
    // `{flip: false}` / `{flip: true}` remove the planner's choice; each
    // pinned query has exactly one plan, and those plans match the two
    // attempts the planner enumerates for the unpinned query.
    const free = executeAllPlanAttempts(unsoldFrankTracks(), true);
    const forcedSemi = executeAllPlanAttempts(unsoldFrankTracks(false), true);
    const forcedFlipped = executeAllPlanAttempts(unsoldFrankTracks(true), true);

    expect(free).toHaveLength(2);
    expect(forcedSemi).toHaveLength(1);
    expect(forcedFlipped).toHaveLength(1);

    const semiAttempt = must(free.find(r => r.flipPattern === 0));
    const flippedAttempt = must(free.find(r => r.flipPattern === 1));
    expect(forcedSemi[0].actualRowsScanned).toBe(semiAttempt.actualRowsScanned);
    expect(forcedFlipped[0].actualRowsScanned).toBe(
      flippedAttempt.actualRowsScanned,
    );
  });

  test('planner beats the forced-unplanned baseline ~200x (indexed db)', () => {
    // As written (flip: false), hydration scans every track (~3503 of them,
    // ~6000 rows vended in total across sources) probing invoiceLines and
    // album for each, because the 5 matching tracks are at the end of the
    // table. The planner instead flips the album join: 1 album -> its 11
    // tracks -> 11 existence probes (~28 rows total).
    const results = executeAllPlanAttempts(unsoldFrankTracks(), true);
    const pick = picked(results);
    const base = baseline(results);

    // The planner chose the flipped plan, and it is the optimal attempt.
    expect(pick.flipPattern).toBe(1);
    expect(pick.actualRowsScanned).toBe(optimal(results).actualRowsScanned);

    // The as-written plan really is a near-full scan...
    expect(base.actualRowsScanned).toBeGreaterThan(3000);
    // ...and the picked plan does a small fraction of its work.
    // (Measured: 28 vs 5988 rows = 0.005; threshold leaves 10x headroom.)
    expect(pick.actualRowsScanned).toBeLessThan(0.05 * base.actualRowsScanned);
  });

  test('without a title index, value-position skew is invisible (documented limitation)', () => {
    // On the base db (no index on album.title) the flipped plan is costed
    // as a full album scan, so the planner keeps the as-written semi plan.
    // The semi plan happens to scan ~6000 rows because the matching tracks
    // are last - but no statistics can see WHERE in the table matches live,
    // so this is the best call available. (The indexed-db test above shows
    // the planner exploiting the index the moment it exists.)
    const results = executeAllPlanAttempts(unsoldFrankTracks());
    expect(picked(results).flipPattern).toBe(0);
  });

  test('planner flips only when flipping actually wins (Big Ones)', () => {
    // Same query shape, but album 'Big Ones' has its tracks near the START
    // of the track table, so the semi plan terminates early and is genuinely
    // cheap. The planner should keep it on the base db and flip on the
    // indexed db - and in both cases its pick must be the optimal attempt.
    const query = queries.track
      .where(({not, exists}) => not(exists('invoiceLines')))
      .whereExists('album', a => a.where('title', 'Big Ones'))
      .limit(5);

    const base = executeAllPlanAttempts(query);
    expect(picked(base).flipPattern).toBe(0);
    expect(picked(base).actualRowsScanned).toBe(
      optimal(base).actualRowsScanned,
    );

    const indexed = executeAllPlanAttempts(query, true);
    expect(picked(indexed).flipPattern).toBe(1);
    expect(picked(indexed).actualRowsScanned).toBe(
      optimal(indexed).actualRowsScanned,
    );
    // Measured: 40 vs 71 rows. Modest here, but it is the cheaper plan.
    expect(picked(indexed).actualRowsScanned).toBeLessThan(
      baseline(indexed).actualRowsScanned,
    );
  });

  test('planner avoids a catastrophic flip inside a junction NOT EXISTS', () => {
    // not(exists('tracks')) on playlist goes through the playlistTrack
    // junction: the outer NOT EXISTS cannot flip, but the inner
    // playlistTrack->track EXISTS can. Flipping it drives the query from
    // the track table and scans ~670x more rows (measured: 63168 vs 94).
    // The planner must keep the semi plan.
    const query = queries.playlist.where(({not, exists}) =>
      not(exists('tracks')),
    );

    for (const useIndexedDb of [false, true]) {
      const results = executeAllPlanAttempts(query, useIndexedDb);
      const pick = picked(results);
      expect(pick.flipPattern).toBe(0);
      expect(pick.actualRowsScanned).toBe(optimal(results).actualRowsScanned);

      const flipped = results.find(r => r.flipPattern === 1);
      expect(
        must(flipped).actualRowsScanned / pick.actualRowsScanned,
      ).toBeGreaterThan(100);

      // The semi plan's estimate must be in the right ballpark. NOT EXISTS
      // executes as a capped existence probe, and the estimate should
      // reflect that (measured: est 36 vs 94 rows actually scanned). Before
      // anti-join costing, each probe was costed as an unlimited scan of
      // the junction table, inflating the estimate ~220x (est 20736).
      expect(pick.estimatedCost).toBeLessThan(10 * pick.actualRowsScanned);
    }
  });

  // All plan variants must produce identical results - flipping a sibling
  // EXISTS is an execution strategy, never a semantics change. Each variant
  // is checked against Postgres (which ignores flip flags entirely).
  test.each([
    {
      name: 'artists with no albums',
      query: () =>
        queries.artist.where(({not, exists}) => not(exists('albums'))),
    },
    {
      name: 'playlists with no tracks',
      query: () =>
        queries.playlist.where(({not, exists}) => not(exists('tracks'))),
    },
    {
      name: 'unsold Frank tracks - as written',
      query: () => unsoldFrankTracks(false),
    },
    {
      name: 'unsold Frank tracks - sibling EXISTS flipped',
      query: () => unsoldFrankTracks(true),
    },
    {
      name: 'unsold Big Ones tracks - sibling EXISTS flipped',
      query: () =>
        queries.track
          .where(({not, exists}) => not(exists('invoiceLines')))
          .whereExists('album', a => a.where('title', 'Big Ones'), {flip: true})
          .orderBy('id', 'asc')
          .limit(5),
    },
  ])('results match Postgres: $name', async ({query}) => {
    await runAndCompare(schema, delegates, query(), undefined);
  });
});
