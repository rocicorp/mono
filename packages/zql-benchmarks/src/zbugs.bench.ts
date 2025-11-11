// oxlint-disable no-console
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
import type {PullRow, Query} from '../../zql/src/query/query.ts';
import {queryWithContext} from '../../zql/src/query/query-internals.ts';
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

// Helper to set flip to undefined in all correlated subquery conditions
function setFlipToUndefined(condition: Condition): Condition {
  if (condition.type === 'correlatedSubquery') {
    return {
      ...condition,
      flip: undefined,
      related: {
        ...condition.related,
        subquery: setFlipToUndefinedInAST(condition.related.subquery),
      },
    };
  } else if (condition.type === 'and' || condition.type === 'or') {
    return {
      ...condition,
      conditions: condition.conditions.map(setFlipToUndefined),
    };
  }
  return condition;
}

function setFlipToUndefinedInAST(ast: AST): AST {
  return {
    ...ast,
    where: ast.where ? setFlipToUndefined(ast.where) : undefined,
    related: ast.related?.map(r => ({
      ...r,
      subquery: setFlipToUndefinedInAST(r.subquery),
    })),
  };
}

// Helper to create a query from an AST
function createQuery<TTable extends keyof typeof schema.tables & string>(
  tableName: TTable,
  queryAST: AST,
) {
  const q = new QueryImpl(schema, tableName, queryAST, defaultFormat, 'test');
  return q as Query<
    typeof schema,
    TTable,
    PullRow<TTable, typeof schema>,
    unknown
  >;
}

// Helper to benchmark planned vs unplanned
function benchmarkQuery<TTable extends keyof typeof schema.tables & string>(
  name: string,
  // oxlint-disable-next-line no-explicit-any
  query: Query<typeof schema, TTable, any>,
) {
  console.log('RUNNING!', name);
  const unplannedAST = queryWithContext(query, undefined).ast;

  // Map to server names, plan, then map back to client names
  const mappedAST = mapAST(unplannedAST, clientToServerMapper);

  // Deep copy mappedAST and set flip to false for all correlated subqueries
  // const mappedASTCopy = setFlipToUndefinedInAST(mappedAST);

  const dbg = new AccumulatorDebugger();
  console.log('Planning query:', name);

  const plannedServerAST = planQuery(mappedAST, costModel, dbg);
  const plannedClientAST = mapAST(plannedServerAST, serverToClientMapper);

  // console.log('Planned ast', JSON.stringify(plannedClientAST, null, 2));
  console.log('Planning debug info:');
  console.log(dbg.format());

  const tableName = unplannedAST.table as TTable;
  const unplannedQuery = createQuery(tableName, unplannedAST);
  const plannedQuery = createQuery(tableName, plannedClientAST);

  summary(() => {
    bench(`unplanned: ${name}`, async () => {
      await delegate.run(unplannedQuery);
    });

    bench(`planned: ${name}`, async () => {
      await delegate.run(plannedQuery);
    });
  });
}

// Benchmark queries from apps/zbugs/shared/queries.ts

// allLabels query
benchmarkQuery('allLabels', builder.label);

// allUsers query
benchmarkQuery('allUsers', builder.user);

// allProjects query
benchmarkQuery('allProjects', builder.project);

// labelsForProject query
benchmarkQuery(
  'labelsForProject - roci',
  builder.label.whereExists('project', q => q.where('lowerCaseName', 'roci')),
);

// issuePreloadV2 query - simplified version
benchmarkQuery(
  'issuePreloadV2 - roci project',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .related('labels')
    .related('viewState', q => q.where('userID', 'test-user'))
    .related('creator')
    .related('assignee')
    .related('emoji', emoji => emoji.related('creator'))
    .related('comments', comments =>
      comments
        .related('creator')
        .related('emoji', emoji => emoji.related('creator'))
        .limit(10)
        .orderBy('created', 'desc'),
    )
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(1000),
);

benchmarkQuery(
  'usersForProject - assignee filter',
  builder.user.whereExists(
    'assignedIssues',
    i => i.whereExists('project', p => p.where('lowerCaseName', 'roci')),
    {flip: true},
  ),
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
