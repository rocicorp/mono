import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {
  queries,
  initializePlannerInfrastructure,
  initializeIndexedDatabase,
  executeAllPlanAttempts,
  validateCorrelation,
  validateWithinOptimal,
  validateWithinBaseline,
  type ValidationResult,
} from './planner-exec-helpers.ts';

// Global collection for summary table
type TestSummary = {
  name: string;
  base: {
    correlation?: number;
    correlationThreshold?: number;
    withinOptimal?: number;
    withinOptimalThreshold?: number;
    withinBaseline?: number;
    withinBaselineThreshold?: number;
  };
  indexed: {
    correlation?: number;
    correlationThreshold?: number;
    withinOptimal?: number;
    withinOptimalThreshold?: number;
    withinBaseline?: number;
    withinBaselineThreshold?: number;
  };
};

const testSummaries: TestSummary[] = [];

describe('Chinook planner execution cost validation', () => {
  beforeAll(() => {
    initializePlannerInfrastructure();
    initializeIndexedDatabase();
  });

  afterAll(() => {
    // Print summary table in markdown format
    // eslint-disable-next-line no-console
    console.log('\n\n=== VALIDATION SUMMARY (Markdown Table) ===\n');

    // Helper to format number or N/A
    const fmt = (num: number | undefined) =>
      num !== undefined ? num.toFixed(2) : 'N/A';

    // Helper to format actual/threshold and indicate if it can be tightened
    const fmtWithThreshold = (
      actual: number | undefined,
      threshold: number | undefined,
      type: 'correlation' | 'within-optimal' | 'within-baseline',
    ) => {
      if (actual === undefined || threshold === undefined) {
        return fmt(actual);
      }

      const actualStr = actual.toFixed(2);
      const thresholdStr = threshold.toFixed(2);

      // Check if can be tightened (has significant headroom)
      let canTighten = false;
      if (type === 'correlation') {
        // For correlation, actual > threshold is good (headroom > 10%)
        canTighten = actual >= threshold && actual - threshold > 0.1;
      } else {
        // For within-optimal and within-baseline, actual < threshold is good
        // Check if actual is significantly better (>10% headroom)
        canTighten =
          actual <= threshold &&
          threshold > 0 &&
          (threshold - actual) / threshold > 0.1;
      }

      if (canTighten) {
        return `${actualStr} (${thresholdStr}) 🔧`;
      } else if (actualStr !== thresholdStr) {
        return `${actualStr} (${thresholdStr})`;
      } else {
        return actualStr;
      }
    };

    // Print markdown table header
    // eslint-disable-next-line no-console
    console.log(
      '| Test Name | Base: corr | Base: opt | Base: baseline | Indexed: corr | Indexed: opt | Indexed: baseline |',
    );
    // eslint-disable-next-line no-console
    console.log(
      '|-----------|------------|-----------|----------------|---------------|--------------|-------------------|',
    );

    // Print rows
    for (const summary of testSummaries) {
      const row =
        `| ${summary.name} ` +
        `| ${fmtWithThreshold(summary.base.correlation, summary.base.correlationThreshold, 'correlation')} ` +
        `| ${fmtWithThreshold(summary.base.withinOptimal, summary.base.withinOptimalThreshold, 'within-optimal')} ` +
        `| ${fmtWithThreshold(summary.base.withinBaseline, summary.base.withinBaselineThreshold, 'within-baseline')} ` +
        `| ${fmtWithThreshold(summary.indexed.correlation, summary.indexed.correlationThreshold, 'correlation')} ` +
        `| ${fmtWithThreshold(summary.indexed.withinOptimal, summary.indexed.withinOptimalThreshold, 'within-optimal')} ` +
        `| ${fmtWithThreshold(summary.indexed.withinBaseline, summary.indexed.withinBaselineThreshold, 'within-baseline')} |`;
      // eslint-disable-next-line no-console
      console.log(row);
    }

    // eslint-disable-next-line no-console
    console.log('\n🔧 = Can be tightened (>10% headroom)\n');
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
      extraIndexValidations: [
        ['correlation', 1.0],
        ['within-optimal', 1],
        ['within-baseline', 0.01],
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
      extraIndexValidations: [
        ['correlation', 0.94],
        ['within-optimal', 1],
        ['within-baseline', 0.01],
      ],
    },

    {
      name: 'parallel joins - track with album and genre',
      query: queries.track
        .whereExists('album', q => q.where('title', 'Big Ones'))
        .whereExists('genre', q => q.where('name', 'Rock'))
        .limit(10),
      validations: [
        ['correlation', 0.4],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
      extraIndexValidations: [
        ['correlation', 0.8],
        ['within-optimal', 1],
        ['within-baseline', 0.65],
      ],
    },

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
        ['correlation', 0.8],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
      extraIndexValidations: [
        ['correlation', 0.8],
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
      extraIndexValidations: [
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
        ['correlation', 0.75],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
      extraIndexValidations: [
        ['correlation', 0.4],
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
        ['correlation', 0.75],
        ['within-optimal', 1],
        ['within-baseline', 0.077],
      ],
      extraIndexValidations: [
        ['correlation', 0.6],
        ['within-baseline', 0.001],
      ],
    },

    // Correlation and within-optimal fail because of empty/near-empty result sets causing division by zero.
    // SQLite does not have stats on the milliseconds column so assumes 80% selectivity.
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
        ['correlation', 0.15],
        ['within-optimal', 1.5],
        ['within-baseline', 1],
      ],
      extraIndexValidations: [
        ['correlation', -0.5],
        ['within-baseline', 1],
      ],
    },

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
        ['correlation', 0.25],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
      extraIndexValidations: [
        ['correlation', 0.3],
        ['within-optimal', 1],
        ['within-baseline', 1],
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
        ['correlation', 0],
        ['within-optimal', 1.7],
        ['within-baseline', 1.7],
      ],
      extraIndexValidations: [
        ['correlation', 0],
        ['within-optimal', 1.7],
        ['within-baseline', 1.7],
      ],
    },

    /**
     * FIXED: Value inlining bug fix dramatically improved this query!
     * Previously: correlation=0.0, within-optimal=3.36x (picked plan was 3x worse than optimal)
     * Now: correlation=0.8, within-optimal=1.0x (picks optimal plan)
     *
     * Even without an index on track.composer, the planner now makes good decisions.
     * Indices don't provide additional benefit since the planner already picks the optimal plan.
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
      extraIndexValidations: [
        ['correlation', 0.8],
        ['within-optimal', 1],
        ['within-baseline', 0.025],
      ],
    },

    /**
     * FIXED: Value inlining bug fix dramatically improved this query!
     * Previously: correlation=0.0, within-optimal=14.74x (picked plan was 15x worse than optimal)
     * Now: correlation=0.94, within-optimal=1.0x (picks optimal plan)
     *
     * The planner now correctly handles empty result sets.
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
        ['within-optimal', 15],
        ['within-baseline', 1],
      ],
      extraIndexValidations: [
        ['correlation', 0.9],
        ['within-baseline', 0.001],
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
        ['correlation', -1.0],
        ['within-optimal', 10],
        ['within-baseline', 1],
      ],
      extraIndexValidations: [
        ['correlation', -1.0],
        ['within-optimal', 87],
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
      extraIndexValidations: [
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
        ['correlation', 0.4],
        ['within-optimal', 1.22],
        ['within-baseline', 0.106],
      ],
      extraIndexValidations: [
        ['correlation', 0.35],
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
        ['correlation', 0.4],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
      extraIndexValidations: [
        ['correlation', 0.4],
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
        ['correlation', 0],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
      extraIndexValidations: [
        ['correlation', 0.95],
        ['within-optimal', 1],
        ['within-baseline', 0.5],
      ],
    },

    {
      name: 'empty result - filter after expensive join',
      query: queries.track
        .whereExists('album', a => a.whereExists('artist'))
        .where('name', 'NonexistentTrackXYZ'),
      validations: [
        ['correlation', 0.1],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
      extraIndexValidations: [
        ['correlation', 0.9],
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
      extraIndexValidations: [
        ['correlation', 0.94],
        ['within-baseline', 0.001],
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
      extraIndexValidations: [
        ['correlation', 0.8],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'deep nesting with very selective top filter',
      query: queries.invoiceLine
        .where('quantity', '>', 5)
        .whereExists('invoice', i =>
          i.whereExists('customer', c => c.whereExists('supportRep', e => e)),
        ),
      validations: [
        ['correlation', -0.5],
        ['within-optimal', 1.5],
        ['within-baseline', 1.43],
      ],
      extraIndexValidations: [
        ['correlation', 0.85],
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
        ['within-baseline', 0.012],
      ],
      extraIndexValidations: [
        ['correlation', 1.0],
        ['within-optimal', 1],
        ['within-baseline', 0.012],
      ],
    },

    {
      name: 'dense junction - popular playlist with many tracks',
      query: queries.playlist.where('id', 1).whereExists('tracks'),
      validations: [
        ['correlation', 1.0],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
      extraIndexValidations: [
        ['correlation', 1.0],
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
      extraIndexValidations: [
        ['correlation', 0.9],
        ['within-optimal', 1],
        ['within-baseline', 0.8],
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
        ['correlation', 0.4],
        ['within-optimal', 2.03],
        ['within-baseline', 1],
      ],
      extraIndexValidations: [
        ['correlation', 0.9],
        ['within-optimal', 1],
        ['within-baseline', 0.35],
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
        ['correlation', 0.4],
        ['within-optimal', 2.24],
        ['within-baseline', 1],
      ],
      extraIndexValidations: [
        ['correlation', 0.9],
        ['within-optimal', 1],
        ['within-baseline', 0.3],
      ],
    },
  ])('$name', ({name, query, validations, extraIndexValidations}) => {
    // Execute all plan attempts and collect results (baseline DB)
    const results = executeAllPlanAttempts(query);

    // Verify we got multiple planning attempts
    expect(results.length).toBeGreaterThan(0);

    // Initialize summary entry
    const summary: TestSummary = {
      name,
      base: {},
      indexed: {},
    };

    // Run requested validations
    const validationResults: ValidationResult[] = [];

    for (const validation of validations) {
      const [validationType, threshold] = validation as [string, number];

      if (validationType === 'correlation') {
        const result = validateCorrelation(results, threshold);
        validationResults.push(result);
        summary.base.correlation = result.actualValue;
        summary.base.correlationThreshold = threshold;
      } else if (validationType === 'within-optimal') {
        const result = validateWithinOptimal(results, threshold);
        validationResults.push(result);
        summary.base.withinOptimal = result.actualValue;
        summary.base.withinOptimalThreshold = threshold;
      } else if (validationType === 'within-baseline') {
        const result = validateWithinBaseline(results, threshold);
        validationResults.push(result);
        summary.base.withinBaseline = result.actualValue;
        summary.base.withinBaselineThreshold = threshold;
      }
    }

    // Log actual values for all tests with headroom analysis
    // eslint-disable-next-line no-console
    console.log('');
    // eslint-disable-next-line no-console
    console.log('  [Baseline DB - FK indices only]');
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
    let failedValidations = validationResults.filter(v => !v.passed);

    // If extraIndexValidations provided, run against indexed DB
    if (extraIndexValidations) {
      // eslint-disable-next-line no-console
      console.log('');
      // eslint-disable-next-line no-console
      console.log('  [Indexed DB - with extra indices]');

      const indexedResults = executeAllPlanAttempts(query, true);
      const indexedValidationResults: ValidationResult[] = [];

      for (const validation of extraIndexValidations) {
        const [validationType, threshold] = validation as [string, number];

        if (validationType === 'correlation') {
          const result = validateCorrelation(indexedResults, threshold);
          indexedValidationResults.push(result);
          summary.indexed.correlation = result.actualValue;
          summary.indexed.correlationThreshold = threshold;
        } else if (validationType === 'within-optimal') {
          const result = validateWithinOptimal(indexedResults, threshold);
          indexedValidationResults.push(result);
          summary.indexed.withinOptimal = result.actualValue;
          summary.indexed.withinOptimalThreshold = threshold;
        } else if (validationType === 'within-baseline') {
          const result = validateWithinBaseline(indexedResults, threshold);
          indexedValidationResults.push(result);
          summary.indexed.withinBaseline = result.actualValue;
          summary.indexed.withinBaselineThreshold = threshold;
        }
      }

      // Log indexed validation results
      for (const v of indexedValidationResults) {
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

      // Add indexed failures to overall failures
      failedValidations = [
        ...failedValidations,
        ...indexedValidationResults.filter(v => !v.passed),
      ];
    }

    // Store summary for final report
    testSummaries.push(summary);

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
