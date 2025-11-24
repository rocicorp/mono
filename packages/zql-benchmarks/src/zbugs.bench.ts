import {bench, run, summary} from 'mitata';
import {createSQLiteCostModel} from '../../zqlite/src/sqlite-cost-model.ts';
import {
  clientToServer,
  serverToClient,
} from '../../zero-schema/src/name-mapper.ts';
import {planQuery} from '../../zql/src/planner/planner-builder.ts';
import {mapAST} from '../../zero-protocol/src/ast.ts';
import type {AST, Condition} from '../../zero-protocol/src/ast.ts';
import {QueryImpl} from '../../zql/src/query/query-impl.ts';
import {defaultFormat} from '../../zql/src/ivm/default-format.ts';
import type {AnyQuery, Query} from '../../zql/src/query/query.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import {expect, test} from 'vitest';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import {Database} from '../../zqlite/src/db.ts';
import {newQueryDelegate} from '../../zqlite/src/test/source-factory.ts';
import {schema, builder} from './schema.ts';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {AccumulatorDebugger} from '../../zql/src/planner/planner-debug.ts';
// import {AccumulatorDebugger} from '../../zql/src/planner/planner-debug.ts';

// Open the zbugs SQLite database
const db = new Database(
  createSilentLogContext(),
  '/Users/mlaw/workspace/mono/apps/zbugs/zbugs-replica.db',
);
const lc = createSilentLogContext();

// Run ANALYZE to populate SQLite statistics for cost model
db.exec('ANALYZE;');

// Get table specs using computeZqlSpecs
const tableSpecs = new Map<string, LiteAndZqlSpec>();
computeZqlSpecs(createSilentLogContext(), db, tableSpecs);

// Create SQLite cost model
const costModel = createSQLiteCostModel(db, tableSpecs);

// Create name mappers
const clientToServerMapper = clientToServer(schema.tables);
const serverToClientMapper = serverToClient(schema.tables);

// Create SQLite delegate
const delegate = newQueryDelegate(lc, testLogConfig, db, schema);

// Helper to set flip to false in all correlated subquery conditions
function setFlipToFalse(condition: Condition): Condition {
  if (condition.type === 'correlatedSubquery') {
    return {
      ...condition,
      flip: false,
      related: {
        ...condition.related,
        subquery: setFlipToFalseInAST(condition.related.subquery),
      },
    };
  } else if (condition.type === 'and' || condition.type === 'or') {
    return {
      ...condition,
      conditions: condition.conditions.map(setFlipToFalse),
    };
  }
  return condition;
}

function setFlipToFalseInAST(ast: AST): AST {
  return {
    ...ast,
    where: ast.where ? setFlipToFalse(ast.where) : undefined,
    related: ast.related?.map(r => ({
      ...r,
      subquery: setFlipToFalseInAST(r.subquery),
    })),
  };
}

// Helper to create a query from an AST
function createQuery<TTable extends keyof typeof schema.tables & string>(
  tableName: TTable,
  queryAST: AST,
) {
  return new QueryImpl(
    schema,
    tableName,
    queryAST,
    defaultFormat,
    'test',
    undefined,
    undefined,
  );
}

// Helper to benchmark planned vs unplanned
async function benchmarkQuery<
  TTable extends keyof typeof schema.tables & string,
>(
  name: string,
  // oxlint-disable-next-line no-explicit-any
  query: Query<typeof schema, TTable, any>,
) {
  const unplannedAST = asQueryInternals(query).ast;

  // Map to server names, plan, then map back to client names
  // const mappedAST = mapAST(unplannedAST, clientToServerMapper);

  // Deep copy mappedAST and set flip to false for all correlated subqueries
  // const mappedASTCopy = setFlipToFalseInAST(mappedAST);

  // const dbg = new AccumulatorDebugger();

  // const plannedServerAST = planQuery(mappedASTCopy, costModel, dbg);
  // const plannedClientAST = mapAST(plannedServerAST, serverToClientMapper);

  // console.log('Planned ast', JSON.stringify(plannedClientAST, null, 2));
  // console.log(dbg.format());

  const tableName = unplannedAST.table as TTable;
  const unplannedQuery = createQuery(tableName, unplannedAST);
  // const plannedQuery = createQuery(tableName, plannedClientAST);

  const start = performance.now();
  await delegate.run(unplannedQuery as AnyQuery);
  const unplannedTime = performance.now() - start;
  console.log('Duration unplanned', name, unplannedTime);

  // summary(() => {
  //   bench(`unplanned: ${name}`, async () => {
  //     // oxlint-disable-next-line no-explicit-any
  //     await delegate.run(unplannedQuery as any);
  //   });

  //   bench(`planned: ${name}`, async () => {
  //     // oxlint-disable-next-line no-explicit-any
  //     await delegate.run(plannedQuery as any);
  //   });
  // });
}

// Benchmark queries from apps/zbugs/shared/queries.ts

// allLabels query
benchmarkQuery(
  'exists',
  builder.issue.whereExists('creator', q => q.where('name', 'sdf')),
);

// Check if JSON output is requested via environment variable
const format = process.env.BENCH_OUTPUT_FORMAT;

if (format === 'json') {
  // Output JSON without samples for smaller, cleaner output
  await run({
    format: {
      json: {
        samples: false,
        debug: false,
      },
    },
  });
} else {
  await run();
}

test('no-op', () => {
  expect(true).toBe(true);
});
