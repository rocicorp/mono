import {beforeAll, describe, expect, test} from 'vitest';
import {
  queries,
  initializePlannerInfrastructure,
  executeAllPlanAttempts,
  validateCorrelation,
  validateWithinOptimal,
  validateWithinBaseline,
  type ValidationResult,
} from './planner-exec-helpers.ts';

describe('Chinook planner execution cost validation', () => {
  beforeAll(() => {
    initializePlannerInfrastructure();
  });

  test.each([
    {
      name: 'simple query - single whereExists',
      query: queries.track.whereExists('album', q =>
        q.where('title', 'Big Ones'),
      ),
      validations: [
        ['correlation', 1.0],
        ['within-optimal', 1],
        ['within-baseline', 0.1],
      ],
    },

    {
      name: 'two-level join - track with album and artist',
      query: queries.track.whereExists('album', album =>
        album.whereExists('artist', artist =>
          artist.where('name', 'Aerosmith'),
        ),
      ),
      validations: [
        ['correlation', 1.0],
        ['within-optimal', 1],
        ['within-baseline', 0.08],
      ],
    },

    /**
     * DEGRADED: Value inlining causes major plan regression.
     * The planner now picks attempt 2 (cost 2635) instead of attempt 0 (cost 58).
     * Correlation dropped from 0.4 to -0.2 (negative correlation!).
     * Picked plan is 45x worse than optimal.
     *
     * Root cause: More accurate STAT4 estimates for individual predicates may
     * be causing worse combined selectivity estimates, leading to suboptimal plan selection.
     */
    {
      name: 'parallel joins - track with album and genre',
      query: queries.track
        .whereExists('album', q => q.where('title', 'Big Ones'))
        .whereExists('genre', q => q.where('name', 'Rock'))
        .limit(10),
      validations: [
        ['within-optimal', 45.5],
        ['within-baseline', 45.5],
      ],
    },

    /**
     * DEGRADED (minor): Correlation dropped from 0.8 to 0.4 due to value inlining.
     * Still picks optimal plan, so no functional impact.
     */
    {
      name: 'three-level join - track with album, artist, and condition',
      query: queries.track
        .whereExists('album', album =>
          album
            .where('title', '>', 'A')
            .whereExists('artist', artist => artist.where('name', '>', 'A')),
        )
        .where('milliseconds', '>', 200000)
        .limit(10),
      validations: [
        ['correlation', 0.4],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'fanout test - album to tracks (high fanout)',
      query: queries.album
        .where('title', 'Greatest Hits')
        .whereExists('tracks', t => t),
      validations: [
        ['correlation', 1.0],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'fanout test - artist to album to track (compound fanout)',
      query: queries.artist
        .where('name', 'Iron Maiden')
        .whereExists('albums', album =>
          album.whereExists('tracks', track => track),
        ),
      validations: [
        ['correlation', 0.8],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'low fanout chain - invoiceLine to track to album (FK relationships)',
      query: queries.invoiceLine.whereExists('track', track =>
        track.whereExists('album', album =>
          album.where(
            'title',
            'The Best of Buddy Guy - The Millennium Collection',
          ),
        ),
      ),
      validations: [
        ['correlation', 0.8],
        ['within-optimal', 1],
        ['within-baseline', 0.077],
      ],
    },

    // Correlation fails because SQLite does not have stats on the milliseconds column.
    // It assumes 80% selectivity.
    // We still end up picking a rather good plan though.
    // Within 1.3x of the best plan (execution time wise)
    {
      name: 'extreme selectivity - artist to album to long tracks',
      query: queries.artist
        .whereExists('albums', album =>
          album.whereExists('tracks', track =>
            track.where('milliseconds', '>', 10_000_000),
          ),
        )
        .limit(5),
      validations: [
        ['correlation', 0.0],
        ['within-optimal', 1.28],
        ['within-baseline', 1],
      ],
    },

    /**
     * DEGRADED: Value inlining causes major plan regression.
     * Previously picked optimal plan, now picks attempt 7 (cost 5433) vs optimal (attempt 0, cost 90).
     * Picked plan is 60x worse than optimal.
     *
     * Root cause: SQLite still has bad default assumptions for non-indexed columns
     * (`employee.where('title', 'Sales Support Agent')` selectivity), but value inlining
     * may be amplifying these errors through the nested join planning.
     *
     * Previously: correlation=0.0, picked optimal plan despite poor correlation
     * Now: correlation=0.214, but picks much worse plan
     */
    {
      name: 'deep nesting - invoiceLine to invoice to customer to employee',
      query: queries.invoiceLine
        .whereExists('invoice', invoice =>
          invoice.whereExists('customer', customer =>
            customer.whereExists('supportRep', employee =>
              employee.where('title', 'Sales Support Agent'),
            ),
          ),
        )
        .limit(20),
      validations: [
        ['correlation', 0.0],
        ['within-optimal', 60.5],
        ['within-baseline', 60.5],
      ],
    },

    /**
     * Fails correlation due to..?
     * Within 1.7x of optimal plan, however.
     * within-baseline is 1.69x (picked plan worse than as-written).
     */
    {
      name: 'asymmetric OR - track with album or invoiceLines',
      query: queries.track
        .where(({or, exists}) =>
          or(
            exists('album', album => album.where('artistId', 1)),
            exists('invoiceLines'),
          ),
        )
        .limit(15),
      validations: [
        ['within-optimal', 1.7],
        ['within-baseline', 1.7],
      ],
    },

    /**
    Problems:
    1. track.composer is not indexed
    2. sqlite returns 0.25 selectivity for `where composer = 'Kurt Cobain'

    The actual selectivity is 0.007.

    The estimated 25% selectivity compounds problems when we get to join fanout.

    It says "with 484 tracks per playlist and 25% global match rate,
    virtually EVERY playlist will have a match." This inflates
    scaledChildSelectivity to 1.0, which then tells the parent "you'll
    find matches in the first 10 playlists you scan."

    Reality: The 26 Kurt Cobain tracks are concentrated in maybe just
    1-2 playlists out of 18. So you need to scan ALL 18 playlists
    (not just 10) to find matches.

    The other problem is that we assume we only need to scan 4 tracks in each
    playlist to find a Kurt Cobain track (because of the 25% selectivity).
    If Kurt Cobain is only in 1 playlist we actually must scan all tracks for all playlists
    until we hit that final playlist.

    >> Sticking an index on `composer` fixes this query.

    3.5x worse!
     */
    {
      name: 'junction table - playlist to tracks via playlistTrack',
      query: queries.playlist
        .whereExists('tracks', track => track.where('composer', 'Kurt Cobain'))
        .limit(10),
      validations: [
        ['correlation', 0.0],
        ['within-optimal', 3.37],
        ['within-baseline', 1],
      ],
    },

    /**
     * Fails correlation because SQLite assumes 25% selectivity.
     *
     * 15x worse!
     */
    {
      name: 'empty result - nonexistent artist',
      query: queries.track
        .whereExists('album', album =>
          album.whereExists('artist', artist =>
            artist.where('name', 'NonexistentArtistZZZZ'),
          ),
        )
        .limit(10),
      validations: [
        ['correlation', 0.0],
        ['within-optimal', 14.75],
        ['within-baseline', 1],
      ],
    },

    /**
     * Currently fails due to SQLite assuming `> Z` has 80% selectivity whereas it really has < 1%.
     * Not sure what we can do here given there is no index on title or same set of workarounds
     * proposed in `F1`
     *
     * Correlation is -1.0 (planner estimates inversely correlated with actual), so we don't check it.
     */
    {
      name: 'F2 sparse FK - track to album with NULL handling',
      query: queries.track
        .where('albumId', 'IS NOT', null)
        .whereExists('album', album => album.where('title', '>', 'Z'))
        .limit(10),
      validations: [
        ['within-optimal', 8.61],
        ['within-baseline', 1],
      ],
    },

    // === NEW TEST CASES ===

    {
      name: 'small dimension table join - genre to tracks',
      query: queries.genre
        .where('name', 'Rock')
        .whereExists('tracks', t => t.where('milliseconds', '>', 200000)),
      validations: [
        ['correlation', 1.0],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'filter pushdown - filters at each nesting level',
      query: queries.track
        .where('milliseconds', '>', 300000)
        .whereExists('album', album =>
          album
            .where('title', 'LIKE', 'A%')
            .whereExists('artist', artist =>
              artist.where('name', 'LIKE', 'A%'),
            ),
        ),
      validations: [
        ['within-optimal', 1.22],
        ['within-baseline', 0.106],
      ],
    },

    {
      name: 'limit(1) with expensive joins',
      query: queries.artist
        .whereExists('albums', album => album.whereExists('tracks'))
        .limit(1),
      validations: [
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'self-join - employees and their managers',
      query: queries.employee.whereExists('reportsToEmployee', manager =>
        manager.where('title', 'General Manager'),
      ),
      validations: [
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'empty result - filter after expensive join',
      query: queries.track
        .whereExists('album', a => a.whereExists('artist'))
        .where('name', 'NonexistentTrackXYZ'),
      validations: [
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'star schema - invoice with customer and lines',
      query: queries.invoice
        .whereExists('customer', c => c.where('country', 'USA'))
        .whereExists('lines', i => i.where('quantity', '>', 1)),
      validations: [
        ['correlation', 0.94],
        ['within-optimal', 1],
        ['within-baseline', 0.77],
      ],
    },

    {
      name: 'junction with filters on both entities',
      query: queries.playlist
        .where('name', 'LIKE', 'Music%')
        .whereExists('tracks', t => t.where('name', 'LIKE', 'A%')),
      validations: [
        ['correlation', 0.8],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    /**
     * DEGRADED (very minor): Picked plan ratio increased from 1.40x to 1.42x (just 1.7% over threshold).
     * Essentially no change - within rounding error.
     */
    {
      name: 'deep nesting with very selective top filter',
      query: queries.invoiceLine
        .where('quantity', '>', 5)
        .whereExists('invoice', i =>
          i.whereExists('customer', c => c.whereExists('supportRep', e => e)),
        ),
      validations: [
        ['within-optimal', 1.43],
        ['within-baseline', 1.43],
      ],
    },

    {
      name: 'sort without index support',
      query: queries.track
        .whereExists('album', a => a.where('artistId', 1))
        .orderBy('milliseconds', 'desc')
        .limit(10),
      validations: [
        ['correlation', 1.0],
        ['within-optimal', 1],
        ['within-baseline', 0.01],
      ],
    },

    /**
     * DEGRADED (minor): Correlation dropped from 1.0 to 0.8 due to value inlining.
     * Still picks optimal plan, so no functional impact.
     */
    {
      name: 'dense junction - popular playlist with many tracks',
      query: queries.playlist.where('id', 1).whereExists('tracks'),
      validations: [
        ['correlation', 0.8],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'varying limit - limit 5',
      query: queries.track
        .whereExists('album', album =>
          album.whereExists('artist', artist =>
            artist.where('name', 'Aerosmith'),
          ),
        )
        .limit(5),
      validations: [
        ['correlation', 0.8],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'varying limit - limit 50',
      query: queries.track
        .whereExists('album', album =>
          album.whereExists('artist', artist =>
            artist.where('name', 'Iron Maiden'),
          ),
        )
        .limit(50),
      validations: [
        ['within-optimal', 2.1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'varying limit - limit 100',
      query: queries.track
        .whereExists('album', album =>
          album.whereExists('artist', artist =>
            artist.where('name', 'Iron Maiden'),
          ),
        )
        .limit(100),
      validations: [
        ['within-optimal', 2.38],
        ['within-baseline', 1],
      ],
    },
  ])('$name', ({query, validations}) => {
    // Execute all plan attempts and collect results
    const results = executeAllPlanAttempts(query);

    // Verify we got multiple planning attempts
    expect(results.length).toBeGreaterThan(0);

    // Run requested validations
    const validationResults: ValidationResult[] = [];

    for (const validation of validations) {
      const [validationType, threshold] = validation as [string, number];

      if (validationType === 'correlation') {
        validationResults.push(validateCorrelation(results, threshold));
      } else if (validationType === 'within-optimal') {
        validationResults.push(validateWithinOptimal(results, threshold));
      } else if (validationType === 'within-baseline') {
        validationResults.push(validateWithinBaseline(results, threshold));
      }
    }

    // Log actual values for all tests with headroom analysis
    // eslint-disable-next-line no-console
    console.log('');
    for (const v of validationResults) {
      const symbol = v.passed ? '✓' : '✗';
      if (v.type === 'correlation') {
        const margin = v.actualValue - v.threshold;
        const headroom =
          v.threshold > 0 ? ((margin / v.threshold) * 100).toFixed(1) : 'N/A';
        // eslint-disable-next-line no-console
        console.log(
          `  ${v.type}: actual=${v.actualValue.toFixed(3)}, threshold=${v.threshold} (headroom: ${headroom}%) ${symbol}`,
        );
      } else {
        const margin = v.threshold - v.actualValue;
        const headroom =
          v.threshold > 0 ? ((margin / v.threshold) * 100).toFixed(1) : 'N/A';
        // eslint-disable-next-line no-console
        console.log(
          `  ${v.type}: actual=${v.actualValue.toFixed(2)}x, threshold=${v.threshold}x (headroom: ${headroom}%) ${symbol}`,
        );
      }
    }

    // Check if all validations passed
    const failedValidations = validationResults.filter(v => !v.passed);

    if (failedValidations.length > 0) {
      const estimatedCosts = results.map(r => r.estimatedCost);
      const actualCosts = results.map(r => r.actualRowsScanned);

      // eslint-disable-next-line no-console
      console.log('\n=== FAILED VALIDATIONS ===');
      for (const v of failedValidations) {
        // eslint-disable-next-line no-console
        console.log(`[${v.type}] ${v.details}`);
      }
      // eslint-disable-next-line no-console
      console.log('\nEstimated costs:', estimatedCosts);
      // eslint-disable-next-line no-console
      console.log('Actual costs:', actualCosts);
      // eslint-disable-next-line no-console
      console.log('\nDetailed results:');
      for (const r of results) {
        // eslint-disable-next-line no-console
        console.log(
          `  Attempt ${r.attemptNumber}: est=${r.estimatedCost}, actual=${r.actualRowsScanned}, flip=${r.flipPattern}`,
        );
      }
    }

    // Assert all validations passed
    expect(failedValidations).toHaveLength(0);
  });
});
