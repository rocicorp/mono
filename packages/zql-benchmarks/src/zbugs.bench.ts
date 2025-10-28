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
import type {Query} from '../../zql/src/query/query.ts';
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
    delegate,
    schema,
    tableName,
    queryAST,
    defaultFormat,
    'test',
  );
}

// Helper to benchmark planned vs unplanned
function benchmarkQuery<TTable extends keyof typeof schema.tables & string>(
  name: string,
  // oxlint-disable-next-line no-explicit-any
  query: Query<typeof schema, TTable, any>,
) {
  const unplannedAST = query.ast;

  // Map to server names, plan, then map back to client names
  const mappedAST = mapAST(unplannedAST, clientToServerMapper);

  // Deep copy mappedAST and set flip to false for all correlated subqueries
  const mappedASTCopy = setFlipToFalseInAST(mappedAST);

  const dbg = new AccumulatorDebugger();

  const plannedServerAST = planQuery(mappedASTCopy, costModel, dbg);
  const plannedClientAST = mapAST(plannedServerAST, serverToClientMapper);

  // console.log('Planned ast', JSON.stringify(plannedClientAST, null, 2));
  console.log(dbg.format());

  const tableName = unplannedAST.table as TTable;
  const unplannedQuery = createQuery(tableName, unplannedAST);
  const plannedQuery = createQuery(tableName, plannedClientAST);

  summary(() => {
    bench(`unplanned: ${name}`, async () => {
      await unplannedQuery.run();
    });

    bench(`planned: ${name}`, async () => {
      await plannedQuery.run();
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

// userPickerV2 query - crew filter
benchmarkQuery(
  'userPickerV2 - crew filter',
  builder.user.where(({cmp, not, and}) =>
    and(cmp('role', 'crew'), not(cmp('login', 'LIKE', 'rocibot%'))),
  ),
);

// userPickerV2 query - creators filter
benchmarkQuery(
  'userPickerV2 - creators filter',
  builder.user.whereExists('createdIssues', i =>
    i.whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    }),
  ),
);

// userPickerV2 query - assignees filter
benchmarkQuery(
  'userPickerV2 - assignees filter',
  builder.user.whereExists('assignedIssues', i =>
    i.whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    }),
  ),
);

// issueDetail query - simplified version
benchmarkQuery(
  'issueDetail - by id',
  builder.issue
    .where('id', 'test-issue-id')
    .related('project')
    .related('emoji', emoji => emoji.related('creator'))
    .related('creator')
    .related('assignee')
    .related('labels')
    .related('viewState', viewState =>
      viewState.where('userID', 'test-user').one(),
    )
    .related('comments', comments =>
      comments
        .related('creator')
        .related('emoji', emoji => emoji.related('creator'))
        .limit(11)
        .orderBy('created', 'desc')
        .orderBy('id', 'desc'),
    )
    .one(),
);

// emojiChange query
benchmarkQuery(
  'emojiChange',
  builder.emoji
    .where('subjectID', 'test-subject-id')
    .related('creator', creator => creator.one()),
);

// issueList queries with various filters

// issueList - basic list with project filter
benchmarkQuery(
  'issueList - roci project, no filters',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {flip: true})
    .related('viewState', q => q.where('userID', 'test-user'))
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(50),
);

// issueList - with open filter
benchmarkQuery(
  'issueList - roci project, open only',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {flip: true})
    .where('open', true)
    .related('viewState', q => q.where('userID', 'test-user'))
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(50),
);

// issueList - with creator filter
// ok... so and should not add
// it should just take parent cardinality?
// since that's the cost to probe with _both_ constraints
// we do not probe twice in and
benchmarkQuery(
  'issueList - roci project, creator filter',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {flip: true})
    .whereExists('creator', q => q.where('login', 'arv'), {flip: true})
    .related('viewState', q => q.where('userID', 'test-user'))
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(50),
);

// issueList - with assignee filter
benchmarkQuery(
  'issueList - roci project, assignee filter',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {flip: true})
    .whereExists('assignee', q => q.where('login', 'arv'), {flip: true})
    .related('viewState', q => q.where('userID', 'test-user'))
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(50),
);

// issueList - with single label filter
benchmarkQuery(
  'issueList - roci project, single label',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {flip: true})
    .whereExists('labels', q => q.where('name', 'bug'), {flip: true})
    .related('viewState', q => q.where('userID', 'test-user'))
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(50),
);

// issueList - with multiple label filters
benchmarkQuery(
  'issueList - roci project, multiple labels',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {flip: true})
    .whereExists('labels', q => q.where('name', 'bug'), {flip: true})
    .whereExists('labels', q => q.where('name', 'high-priority'), {flip: true})
    .related('viewState', q => q.where('userID', 'test-user'))
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(50),
);

// // issueList - with text filter (title search)
// // benchmarkQuery(
// //   'issueList - roci project, text filter',
// //   builder.issue
// //     .whereExists(
// //       'project',
// //       p => p.where('lowerCaseName', 'roci'),
// //       {flip: true},
// //     )
// //     .where(({or, cmp, exists}) =>
// //       or(
// //         cmp('title', 'ILIKE', '%sync%'),
// //         cmp('description', 'ILIKE', '%sync%'),
// //         exists('comments', q => q.where('body', 'ILIKE', '%sync%')),
// //       ),
// //     )
// //     .related('viewState', q => q.where('userID', 'test-user'))
// //     .related('labels')
// //     .orderBy('modified', 'desc')
// //     .orderBy('id', 'desc')
// //     .limit(50),
// // );

// issueList - complex filter combination (open + creator + label)
benchmarkQuery(
  'issueList - roci project, complex filters',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {flip: true})
    .where('open', true)
    .whereExists('creator', q => q.where('login', 'arv'), {flip: true})
    .whereExists('labels', q => q.where('name', 'bug'), {flip: true})
    .related('viewState', q => q.where('userID', 'test-user'))
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(50),
);

// issueList - sorted by created instead of modified
benchmarkQuery(
  'issueList - roci project, sorted by created',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {flip: true})
    .related('viewState', q => q.where('userID', 'test-user'))
    .related('labels')
    .orderBy('created', 'desc')
    .orderBy('id', 'desc')
    .limit(50),
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
