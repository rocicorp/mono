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
    },
    {
      name: 'two-level join - track with album and artist',
      query: queries.track.whereExists('album', album =>
        album.whereExists('artist', artist =>
          artist.where('name', 'Aerosmith'),
        ),
      ),
    },
    {
      name: 'parallel joins - track with album and genre',
      query: queries.track
        .whereExists('album', q => q.where('title', 'Big Ones'))
        .whereExists('genre', q => q.where('name', 'Rock'))
        .limit(10),
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
    },
    {
      name: 'fanout test - album to tracks (high fanout)',
      query: queries.album
        .where('title', 'Greatest Hits')
        .whereExists('tracks', t => t),
    },
    {
      name: 'fanout test - artist to album to track (compound fanout)',
      query: queries.artist
        .where('name', 'Iron Maiden')
        .whereExists('albums', album =>
          album.whereExists('tracks', track => track),
        ),
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
    },
    // TODO: why do you fail?
    // {
    //   name: 'extreme selectivity - artist to album to long tracks',
    //   query: queries.artist
    //     .whereExists('albums', album =>
    //       album.whereExists('tracks', track =>
    //         track.where('milliseconds', '>', 10_000_000),
    //       ),
    //     )
    //     .limit(5),
    // },

    /**
     * ~~ F1
     * Currently fails due to bad default assumptions
     * SQLite assumes `employee.where('title', 'Sales Support Agent')` returns 2 rows
     * but it really returns 11. This is a 5.5x cost factor skew.
     * There is no index on title.
     * We can:
     * - try to gather stats on all columns
     * - try to guess at a better sane default for inequality selectivity (e.g., use PG's default)
     * - workaround! Give the user a util to run all forms of their query and return the optimal query they can ship to prod!
     */
    // {
    //   name: 'F1 deep nesting - invoiceLine to invoice to customer to employee',
    //   query: queries.invoiceLine
    //     .whereExists('invoice', invoice =>
    //       invoice.whereExists('customer', customer =>
    //         customer.whereExists('supportRep', employee =>
    //           employee.where('title', 'Sales Support Agent'),
    //         ),
    //       ),
    //     )
    //     .limit(20),
    // },
    // TODO: why do you fail?
    // {
    //   name: 'asymmetric OR - track with album or invoiceLines',
    //   query: queries.track
    //     .where(({or, exists}) =>
    //       or(
    //         exists('album', album => album.where('artistId', 1)),
    //         exists('invoiceLines'),
    //       ),
    //     )
    //     .limit(15),
    // },
    // TODO: why do you fail?
    // {
    //   name: 'junction table - playlist to tracks via playlistTrack',
    //   query: queries.playlist
    //     .whereExists('tracks', track => track.where('composer', 'Kurt Cobain'))
    //     .limit(10),
    // },
    // TODO: why do you fail?
    // {
    //   name: 'empty result - nonexistent artist',
    //   query: queries.track
    //     .whereExists('album', album =>
    //       album.whereExists('artist', artist =>
    //         artist.where('name', 'NonexistentArtistZZZZ'),
    //       ),
    //     )
    //     .limit(10),
    // },

    /**
     * ~~ F2
     * Currently fails due to SQLite assuming `> Z` has 80% selectivity whereas it really has < 1%.
     * Not sure what we can do here given there is no index on title or same set of workarounds
     * proposed in `F1`
     */
    // {
    //   name: 'F2 sparse FK - track to album with NULL handling',
    //   query: queries.track
    //     .where('albumId', 'IS NOT', null)
    //     .whereExists('album', album => album.where('title', '>', 'Z'))
    //     .limit(10),
    // },
  ])('$name', ({query}) => {
    // Execute all plan attempts and collect results
    const results = executeAllPlanAttempts(query);

    // Verify we got multiple planning attempts
    expect(results.length).toBeGreaterThan(0);

    // Calculate Spearman rank correlation
    const estimatedCosts = results.map(r => r.estimatedCost);
    const actualCosts = results.map(r => r.actualRowsScanned);
    const correlation = spearmanCorrelation(estimatedCosts, actualCosts);

    if (correlation < 0.7) {
      // console.log('\n=== FAILED TEST:', query);
      // console.log('Estimated costs:', estimatedCosts);
      // console.log('Actual costs:', actualCosts);
      // console.log('Correlation:', correlation);
      // console.log('Results:');
      // for (const r of results) {
      //   console.log(
      //     `  Attempt ${r.attemptNumber}: est=${r.estimatedCost}, actual=${r.actualRowsScanned}, flip=${r.flipPattern}`,
      //   );
      // }
    }

    // Assert that correlation is positive and reasonably strong
    // A correlation >= 0.7 indicates the cost model is directionally correct
    expect(correlation).toBeGreaterThanOrEqual(0.7);
  });
});
