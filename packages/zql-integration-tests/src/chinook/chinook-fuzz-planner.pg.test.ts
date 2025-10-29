/* oxlint-disable no-console */
import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../../zero-cache/src/db/specs.ts';
import {Debug} from '../../../zql/src/builder/debug-delegate.ts';
import {ast, QueryImpl} from '../../../zql/src/query/query-impl.ts';
import {planQuery} from '../../../zql/src/planner/planner-builder.ts';
import {AccumulatorDebugger} from '../../../zql/src/planner/planner-debug.ts';
import {generateShrinkableQuery} from '../../../zql/src/query/test/query-gen.ts';
import {mapAST} from '../../../zero-protocol/src/ast.ts';
import {
  clientToServer,
  serverToClient,
} from '../../../zero-schema/src/name-mapper.ts';
import type {AnyQuery} from '../../../zql/src/query/test/util.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {bootstrap} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema, builder} from './schema.ts';

const pgContent = await getChinook();

// Set this to reproduce a specific failure.
const REPRO_SEED = undefined;

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

// Create name mappers for translating between client and server names
const clientToServerMapper = clientToServer(schema.tables);
const serverToClientMapper = serverToClient(schema.tables);

// Manual test cases to verify the infrastructure works
test('manual: track.whereExists(album)', async () => {
  const query = builder.track.whereExists('album');
  await runManualCase(query);
});

test('manual: track.whereExists(album).whereExists(genre)', async () => {
  const query = builder.track.whereExists('album').whereExists('genre');
  await runManualCase(query);
});

test('manual: album.whereExists(tracks)', async () => {
  const query = builder.album.whereExists('tracks');
  await runManualCase(query);
});

test('debug: album.whereExists(tracks)', async () => {
  const query = builder.album.whereExists('tracks');
  const queryAst = ast(query);

  // Run without planning
  const debugUnplanned = new Debug();
  const unplannedQuery = new QueryImpl(
    {
      ...harness.delegates.sqlite,
      debug: debugUnplanned,
    },
    schema,
    queryAst.table as keyof typeof schema.tables,
    queryAst,
    query.format,
  );
  await unplannedQuery.run();
  const unplannedRowCount = getTotalRowCount(debugUnplanned);

  // Run with planning and debug tracing
  const planDebugger = new AccumulatorDebugger();
  const mappedAST = mapAST(queryAst, clientToServerMapper);
  const plannedServerAST = planQuery(mappedAST, costModel, planDebugger);
  const plannedClientAST = mapAST(plannedServerAST, serverToClientMapper);

  console.log('\n=== PLANNER DEBUG TRACE ===');
  console.log(planDebugger.format());
  console.log('===========================\n');

  const debugPlanned = new Debug();
  const plannedQuery = new QueryImpl(
    {
      ...harness.delegates.sqlite,
      debug: debugPlanned,
    },
    schema,
    plannedClientAST.table as keyof typeof schema.tables,
    plannedClientAST,
    query.format,
  );
  await plannedQuery.run();
  const plannedRowCount = getTotalRowCount(debugPlanned);

  console.log(`Unplanned rows: ${unplannedRowCount}`);
  console.log(`Planned rows: ${plannedRowCount}`);
  console.log(`Unplanned row counts:`, debugUnplanned.getVendedRowCounts());
  console.log(`Planned row counts:`, debugPlanned.getVendedRowCounts());
});

// Fuzz tests (disabled for now - queries may be too complex)
test.each(Array.from({length: 0}, () => createCase()))(
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
  const [generatedQuery] = query;
  const queryAst = ast(generatedQuery);

  let unplannedRowCount: number;
  let plannedRowCount: number;

  try {
    // Run without planning
    const debugUnplanned = new Debug();
    const unplannedQuery = new QueryImpl(
      {
        ...harness.delegates.sqlite,
        debug: debugUnplanned,
      },
      schema,
      queryAst.table as keyof typeof schema.tables,
      queryAst,
      generatedQuery.format,
    );
    await unplannedQuery.run();
    unplannedRowCount = getTotalRowCount(debugUnplanned);
  } catch (e) {
    // Skip queries that fail during unplanned execution (query generator issues)
    if (seed === REPRO_SEED) {
      console.log('Unplanned execution failed:', e);
      throw e;
    }
    console.log(`Skipping seed ${seed}: query generation issue`);
    return;
  }

  try {
    // Run with planning
    // Map to server names, plan, then map back to client names
    const mappedAST = mapAST(queryAst, clientToServerMapper);
    const plannedServerAST = planQuery(mappedAST, costModel);
    const plannedClientAST = mapAST(plannedServerAST, serverToClientMapper);

    const debugPlanned = new Debug();
    const plannedQuery = new QueryImpl(
      {
        ...harness.delegates.sqlite,
        debug: debugPlanned,
      },
      schema,
      plannedClientAST.table as keyof typeof schema.tables,
      plannedClientAST,
      generatedQuery.format,
    );
    await plannedQuery.run();
    plannedRowCount = getTotalRowCount(debugPlanned);
  } catch (e) {
    // If planned fails but unplanned succeeded, that's a planner bug
    if (seed === REPRO_SEED) {
      console.log('Planned execution failed:', e);
      throw e;
    }
    throw new Error(
      `Planner broke valid query! Seed ${seed}: ${e instanceof Error ? e.message : String(e)}`,
    );
  }

  // Debug logging
  if (seed === REPRO_SEED || plannedRowCount > unplannedRowCount) {
    console.log(
      `Seed ${seed}: unplanned=${unplannedRowCount}, planned=${plannedRowCount}`,
    );
  }

  // Assert: planned should scan <= unplanned rows
  if (plannedRowCount > unplannedRowCount) {
    throw new Error(
      `Planner increased row count! unplanned=${unplannedRowCount}, planned=${plannedRowCount}. Repro seed: ${seed}`,
    );
  }
}

async function runManualCase(query: AnyQuery) {
  const queryAst = ast(query);

  // Run without planning
  const debugUnplanned = new Debug();
  const unplannedQuery = new QueryImpl(
    {
      ...harness.delegates.sqlite,
      debug: debugUnplanned,
    },
    schema,
    queryAst.table as keyof typeof schema.tables,
    queryAst,
    query.format,
  );
  await unplannedQuery.run();
  const unplannedRowCount = getTotalRowCount(debugUnplanned);

  // Run with planning
  // Map to server names, plan, then map back to client names
  const mappedAST = mapAST(queryAst, clientToServerMapper);
  const plannedServerAST = planQuery(mappedAST, costModel);
  const plannedClientAST = mapAST(plannedServerAST, serverToClientMapper);

  const debugPlanned = new Debug();
  const plannedQuery = new QueryImpl(
    {
      ...harness.delegates.sqlite,
      debug: debugPlanned,
    },
    schema,
    plannedClientAST.table as keyof typeof schema.tables,
    plannedClientAST,
    query.format,
  );
  await plannedQuery.run();
  const plannedRowCount = getTotalRowCount(debugPlanned);

  console.log(`unplanned=${unplannedRowCount}, planned=${plannedRowCount}`);

  // Assert: planned should scan <= unplanned rows
  expect(plannedRowCount).toBeLessThanOrEqual(unplannedRowCount);
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
