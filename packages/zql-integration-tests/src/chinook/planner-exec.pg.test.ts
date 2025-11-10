import {beforeAll, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../../zero-cache/src/db/specs.ts';
import {
  clientToServer,
  type NameMapper,
} from '../../../zero-schema/src/name-mapper.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {AccumulatorDebugger} from '../../../zql/src/planner/planner-debug.ts';
import {
  buildPlanGraph,
  applyPlansToAST,
  planQuery,
} from '../../../zql/src/planner/planner-builder.ts';
import {mapAST} from '../../../zero-protocol/src/ast.ts';
import {
  runtimeDebugFlags,
  Debug,
} from '../../../zql/src/builder/debug-delegate.ts';
import {buildPipeline} from '../../../zql/src/builder/builder.ts';
import {hydrate} from '../../../zero-cache/src/services/view-syncer/pipeline-driver.ts';
import {hashOfAST} from '../../../zero-protocol/src/query-hash.ts';
import {bootstrap} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';
import {spearmanCorrelation} from '../helpers/correlation.ts';
import {queryWithContext} from '../../../zql/src/query/query-internals.ts';

const pgContent = await getChinook();

const {dbs, queries, delegates} = await bootstrap({
  suiteName: 'chinook_planner_exec',
  pgContent,
  zqlSchema: schema,
});

let costModel: ReturnType<typeof createSQLiteCostModel>;
let mapper: NameMapper;
let tableSpecs: Map<string, LiteAndZqlSpec>;

type PlanAttemptResult = {
  attemptNumber: number;
  estimatedCost: number;
  actualRowsScanned: number;
  flipPattern: number;
};

type PlanValidation = 'correlation' | 'cost-tolerance' | 'better-than-baseline';

type ValidationResult = {
  type: PlanValidation;
  passed: boolean;
  details: string;
};

/**
 * Sum all row counts from Debug.getNVisitCounts()
 */
function sumRowCounts(
  nvisitCounts: Record<string, Record<string, number>>,
): number {
  let total = 0;
  for (const tableQueries of Object.values(nvisitCounts)) {
    for (const count of Object.values(tableQueries)) {
      total += count;
    }
  }
  return total;
}

/**
 * Validate correlation between estimated costs and actual costs using Spearman correlation
 */
function validateCorrelation(results: PlanAttemptResult[]): ValidationResult {
  const estimatedCosts = results.map(r => r.estimatedCost);
  const actualCosts = results.map(r => r.actualRowsScanned);
  const correlation = spearmanCorrelation(estimatedCosts, actualCosts);
  const passed = correlation >= 0.7;

  const details = passed
    ? `Spearman correlation ${correlation.toFixed(3)} >= 0.7 threshold`
    : `Spearman correlation ${correlation.toFixed(3)} < 0.7 threshold`;

  return {
    type: 'correlation',
    passed,
    details,
  };
}

/**
 * Validate that the picked plan (lowest estimated cost) is within tolerance
 * of the optimal plan (lowest actual rows scanned)
 */
function validatePickedVsOptimal(
  results: PlanAttemptResult[],
  toleranceFactor: number,
): ValidationResult {
  // Find the picked plan (lowest estimated cost)
  const pickedPlan = results.reduce((best, current) =>
    current.estimatedCost < best.estimatedCost ? current : best,
  );

  // Find the optimal plan (lowest actual rows scanned)
  const optimalPlan = results.reduce((best, current) =>
    current.actualRowsScanned < best.actualRowsScanned ? current : best,
  );

  // Calculate ratio
  const ratio = pickedPlan.actualRowsScanned / optimalPlan.actualRowsScanned;
  const passed = ratio <= toleranceFactor;

  const details = passed
    ? `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} is within ${toleranceFactor}x of optimal (attempt ${optimalPlan.attemptNumber}) cost ${optimalPlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`
    : `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} exceeds ${toleranceFactor}x tolerance of optimal (attempt ${optimalPlan.attemptNumber}) cost ${optimalPlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`;

  return {
    type: 'cost-tolerance',
    passed,
    details,
  };
}

/**
 * Validate that the picked plan (lowest estimated cost) performs better than
 * the baseline query-as-written (attempt 0)
 */
function validateBetterThanBaseline(
  results: PlanAttemptResult[],
): ValidationResult {
  // Find the baseline plan (attempt 0 - query as written)
  const baselinePlan = results.find(r => r.attemptNumber === 0);
  if (!baselinePlan) {
    throw new Error('Baseline plan (attempt 0) not found in results');
  }

  // Find the picked plan (lowest estimated cost)
  const pickedPlan = results.reduce((best, current) =>
    current.estimatedCost < best.estimatedCost ? current : best,
  );

  // If the picked plan IS the baseline, that's OK - the planner didn't make it worse
  if (pickedPlan.attemptNumber === baselinePlan.attemptNumber) {
    return {
      type: 'better-than-baseline',
      passed: true,
      details: `Picked plan is the baseline (attempt ${baselinePlan.attemptNumber}) - no optimization applied`,
    };
  }

  // Check if picked plan is better than baseline
  const passed = pickedPlan.actualRowsScanned <= baselinePlan.actualRowsScanned;
  const ratio =
    baselinePlan.actualRowsScanned > 0
      ? pickedPlan.actualRowsScanned / baselinePlan.actualRowsScanned
      : 1;

  const details = passed
    ? `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} is better than baseline (attempt ${baselinePlan.attemptNumber}) cost ${baselinePlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`
    : `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} is worse than baseline (attempt ${baselinePlan.attemptNumber}) cost ${baselinePlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`;

  return {
    type: 'better-than-baseline',
    passed,
    details,
  };
}

/**
 * Execute all planning attempts for a query and measure estimated vs actual costs
 */
function executeAllPlanAttempts(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  query: any,
): PlanAttemptResult[] {
  // Get the query AST
  const ast = queryWithContext(query, undefined).ast;
  const mappedAST = mapAST(ast, mapper);

  // Plan with debugger to collect all attempts
  const planDebugger = new AccumulatorDebugger();
  planQuery(mappedAST, costModel, planDebugger);

  // Get all completed plan attempts
  const planCompleteEvents = planDebugger.getEvents('plan-complete');

  const results: PlanAttemptResult[] = [];

  // Execute each plan variant
  for (const planEvent of planCompleteEvents) {
    // Rebuild the plan graph for this attempt
    const plans = buildPlanGraph(mappedAST, costModel, true);

    // Restore the exact plan state from the snapshot
    plans.plan.restorePlanningSnapshot(planEvent.planSnapshot);

    // Apply plans to AST to get variant with flip flags set
    const astWithFlips = applyPlansToAST(mappedAST, plans);

    // Enable row count tracking
    runtimeDebugFlags.trackRowCountsVended = true;
    const debug = new Debug();
    delegates.sqlite.debug = debug;

    try {
      // Build pipeline
      delegates.sqlite.mapAst = undefined;
      const pipeline = buildPipeline(
        astWithFlips,
        delegates.sqlite,
        `query-${planEvent.attemptNumber}`,
      );

      // Execute query
      for (const _rowChange of hydrate(
        pipeline,
        hashOfAST(astWithFlips),
        tableSpecs,
      )) {
        // Consume rows to execute the query
      }

      // Collect actual row counts
      const nvisitCounts = debug.getNVisitCounts();
      const actualRowsScanned = sumRowCounts(nvisitCounts);

      results.push({
        attemptNumber: planEvent.attemptNumber,
        estimatedCost: planEvent.totalCost,
        actualRowsScanned,
        flipPattern: planEvent.flipPattern,
      });
    } finally {
      // Disable tracking for next iteration
      runtimeDebugFlags.trackRowCountsVended = false;
    }
  }

  return results;
}

describe('Chinook planner execution cost validation', () => {
  beforeAll(() => {
    mapper = clientToServer(schema.tables);
    dbs.sqlite.exec('ANALYZE;');

    // Get table specs using computeZqlSpecs
    tableSpecs = new Map<string, LiteAndZqlSpec>();
    computeZqlSpecs(createSilentLogContext(), dbs.sqlite, tableSpecs);

    costModel = createSQLiteCostModel(dbs.sqlite, tableSpecs);
  });

  test.each([
    {
      name: 'simple query - single whereExists',
      query: queries.track.whereExists('album', q =>
        q.where('title', 'Big Ones'),
      ),
      validations: ['correlation', 'cost-tolerance', 'better-than-baseline'],
      toleranceFactor: 1,
    },

    {
      name: 'two-level join - track with album and artist',
      query: queries.track.whereExists('album', album =>
        album.whereExists('artist', artist =>
          artist.where('name', 'Aerosmith'),
        ),
      ),
      validations: ['correlation', 'cost-tolerance', 'better-than-baseline'],
      toleranceFactor: 1,
    },

    {
      name: 'parallel joins - track with album and genre',
      query: queries.track
        .whereExists('album', q => q.where('title', 'Big Ones'))
        .whereExists('genre', q => q.where('name', 'Rock'))
        .limit(10),
      validations: ['correlation', 'cost-tolerance', 'better-than-baseline'],
      toleranceFactor: 1,
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
      validations: ['correlation', 'cost-tolerance', 'better-than-baseline'],
      toleranceFactor: 1,
    },

    {
      name: 'fanout test - album to tracks (high fanout)',
      query: queries.album
        .where('title', 'Greatest Hits')
        .whereExists('tracks', t => t),
      validations: ['correlation', 'cost-tolerance', 'better-than-baseline'],
      toleranceFactor: 1,
    },

    {
      name: 'fanout test - artist to album to track (compound fanout)',
      query: queries.artist
        .where('name', 'Iron Maiden')
        .whereExists('albums', album =>
          album.whereExists('tracks', track => track),
        ),
      validations: ['correlation', 'cost-tolerance', 'better-than-baseline'],
      toleranceFactor: 1,
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
      validations: ['correlation', 'cost-tolerance', 'better-than-baseline'],
      toleranceFactor: 1,
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
      validations: ['cost-tolerance', 'better-than-baseline'],
      toleranceFactor: 1.3,
    },

    /**
     * Currently fails correlation due to bad default assumptions
     * SQLite assumes `employee.where('title', 'Sales Support Agent')` returns 2 rows
     * but it really returns 11. This is a 5.5x cost factor skew.
     * There is no index on title.
     * We can:
     * - try to gather stats on all columns
     * - try to guess at a better sane default for inequality selectivity (e.g., use PG's default)
     * - workaround! Give the user a util to run all forms of their query and return the optimal query they can ship to prod!
     *
     * Still ends up picking the best plan, however.
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
      validations: ['cost-tolerance', 'better-than-baseline'],
      toleranceFactor: 1,
    },

    /**
     * Fails correlation due to..?
     * Within 1.7x of optimal plan, however.
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
      validations: ['cost-tolerance'],
      toleranceFactor: 1.7,
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
      validations: ['cost-tolerance', 'better-than-baseline'],
      toleranceFactor: 3.5,
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
      validations: ['cost-tolerance', 'better-than-baseline'],
      toleranceFactor: 15,
    },

    /**
     * Currently fails due to SQLite assuming `> Z` has 80% selectivity whereas it really has < 1%.
     * Not sure what we can do here given there is no index on title or same set of workarounds
     * proposed in `F1`
     */
    {
      name: 'F2 sparse FK - track to album with NULL handling',
      query: queries.track
        .where('albumId', 'IS NOT', null)
        .whereExists('album', album => album.where('title', '>', 'Z'))
        .limit(10),
      validations: ['cost-tolerance', 'better-than-baseline'],
      toleranceFactor: 8.75,
    },
  ])('$name', ({query, validations, toleranceFactor}) => {
    // Execute all plan attempts and collect results
    const results = executeAllPlanAttempts(query);

    // Verify we got multiple planning attempts
    expect(results.length).toBeGreaterThan(0);

    // Run requested validations
    const validationResults: ValidationResult[] = [];

    for (const validation of validations) {
      if (validation === 'correlation') {
        validationResults.push(validateCorrelation(results));
      } else if (validation === 'cost-tolerance') {
        if (toleranceFactor === undefined) {
          throw new Error(
            'toleranceFactor must be specified when using cost-tolerance validation',
          );
        }
        validationResults.push(
          validatePickedVsOptimal(results, toleranceFactor),
        );
      } else if (validation === 'better-than-baseline') {
        validationResults.push(validateBetterThanBaseline(results));
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
