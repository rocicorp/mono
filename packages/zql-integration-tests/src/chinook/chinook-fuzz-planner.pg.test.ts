/* oxlint-disable no-console */
import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../../zero-cache/src/db/specs.ts';
import {Debug} from '../../../zql/src/builder/debug-delegate.ts';
import {buildPipeline} from '../../../zql/src/builder/builder.ts';
import {ast} from '../../../zql/src/query/query-impl.ts';
import {generateShrinkableQuery} from '../../../zql/src/query/test/query-gen.ts';
import type {
  AnyQuery,
  AnyStaticQuery,
} from '../../../zql/src/query/test/util.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {astToZQL} from '../../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../../ast-to-zql/src/format.ts';
import {bootstrap} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';

const pgContent = await getChinook();

// Set this to reproduce a specific failure.
const REPRO_SEED = 1326382354;

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_planner',
  zqlSchema: schema,
  pgContent,
});

// Run ANALYZE to populate SQLite statistics for the cost model
harness.dbs.sqlite.exec('ANALYZE;');

// Get table specs using computeZqlSpecs
const tableSpecs = new Map<string, LiteAndZqlSpec>();
computeZqlSpecs(createSilentLogContext(), harness.dbs.sqlite, tableSpecs);

// Create SQLite cost model
const costModel = createSQLiteCostModel(harness.dbs.sqlite, tableSpecs);

test.each(Array.from({length: 10}, () => createCase()))(
  'fuzz-planner $seed',
  runCase,
);

test('sentinel', () => {
  expect(true).toBe(true);
});

if (REPRO_SEED) {
  // oxlint-disable-next-line no-focused-tests
  test.only('repro', async () => {
    const tc = createCase(REPRO_SEED);
    const {query} = tc;
    console.log(
      'ZQL',
      await formatOutput(ast(query[0]).table + astToZQL(ast(query[0]))),
    );
    await runCase(tc);
  });
}

function createCase(seed?: number) {
  seed = seed ?? Date.now() ^ (Math.random() * 0x100000000);
  const randomizer = generateMersenne53Randomizer(seed);
  const rng = () => randomizer.next();
  const faker = new Faker({
    locale: en,
    randomizer,
  });
  return {
    seed,
    query: generateShrinkableQuery(
      schema,
      {},
      rng,
      faker,
      harness.delegates.pg.serverSchema,
    ),
  };
}

async function runCase({
  query,
  seed,
}: {
  query: [AnyQuery, AnyQuery[]];
  seed: number;
}) {
  try {
    const [generatedQuery] = query;
    const queryAst = ast(generatedQuery);

    // Run without planning
    const debugUnplanned = new Debug();
    const pipelineUnplanned = buildPipeline(
      queryAst,
      {
        ...harness.delegates.sqlite,
        debug: debugUnplanned,
      },
      'query-unplanned',
      undefined, // No cost model = no planning
    );
    await pipelineUnplanned.hydrate();
    const unplannedRowCount = getTotalRowCount(debugUnplanned);

    // Run with planning
    const debugPlanned = new Debug();
    const pipelinePlanned = buildPipeline(
      queryAst,
      {
        ...harness.delegates.sqlite,
        debug: debugPlanned,
      },
      'query-planned',
      costModel, // With cost model = planning enabled
    );
    await pipelinePlanned.hydrate();
    const plannedRowCount = getTotalRowCount(debugPlanned);

    // Debug logging
    if (seed === REPRO_SEED || plannedRowCount > unplannedRowCount) {
      console.log(
        `Seed ${seed}: unplanned=${unplannedRowCount}, planned=${plannedRowCount}`,
      );
    }

    // Assert: planned should scan <= unplanned rows
    expect(plannedRowCount).toBeLessThanOrEqual(unplannedRowCount);
  } catch (e) {
    const zql = await shrink(query[1], seed);
    if (seed === REPRO_SEED) {
      throw e;
    }

    throw new Error(
      'Planner did not improve row count. Repro seed: ' +
        seed +
        '\nshrunk zql: ' +
        zql,
    );
  }
}

function getTotalRowCount(debug: Debug): number {
  const counts = debug.getVendedRowCounts();
  let total = 0;
  for (const tableQueries of Object.values(counts)) {
    for (const count of Object.values(tableQueries)) {
      total += count;
    }
  }
  return total;
}

async function shrink(generations: AnyQuery[], seed: number) {
  console.log('Found failure at seed', seed);
  console.log('Shrinking', generations.length, 'generations');
  let low = 0;
  let high = generations.length;
  let lastFailure = -1;
  while (low < high) {
    const mid = low + ((high - low) >> 1);
    try {
      const queryAst = ast(generations[mid]);

      // Run without planning
      const debugUnplanned = new Debug();
      const pipelineUnplanned = buildPipeline(
        queryAst,
        {
          ...harness.delegates.sqlite,
          debug: debugUnplanned,
        },
        'query-unplanned',
        undefined,
      );
      await pipelineUnplanned.hydrate();
      const unplannedRowCount = getTotalRowCount(debugUnplanned);

      // Run with planning
      const debugPlanned = new Debug();
      const pipelinePlanned = buildPipeline(
        queryAst,
        {
          ...harness.delegates.sqlite,
          debug: debugPlanned,
        },
        'query-planned',
        costModel,
      );
      await pipelinePlanned.hydrate();
      const plannedRowCount = getTotalRowCount(debugPlanned);

      // Debug logging
      console.log(
        `  gen[${mid}]: unplanned=${unplannedRowCount}, planned=${plannedRowCount}, fail=${plannedRowCount > unplannedRowCount}`,
      );

      // Check if this generation fails
      if (plannedRowCount > unplannedRowCount) {
        // Still failing, try earlier generation
        lastFailure = mid;
        high = mid;
      } else {
        // Passes, try later generation
        low = mid + 1;
      }
    } catch {
      // If there's an error running the query, try later generation
      low = mid + 1;
    }
  }
  if (lastFailure === -1) {
    throw new Error('no failure found');
  }
  const query = generations[lastFailure];
  return formatOutput(ast(query).table + astToZQL(ast(query)));
}
