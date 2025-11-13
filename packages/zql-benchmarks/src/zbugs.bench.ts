// oxlint-disable no-console
import {run} from 'mitata';
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
import {
  runtimeDebugFlags,
  Debug,
} from '../../zql/src/builder/debug-delegate.ts';

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

// Helper to sum all row counts from Debug.getNVisitCounts()
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

// Helper to execute query and measure actual rows scanned
async function executeWithRowTracking<
  TTable extends keyof typeof schema.tables & string,
>(
  tableName: TTable,
  ast: AST,
): Promise<{
  duration: number;
  rowsScanned: number;
}> {
  // Create query from AST
  const q = new QueryImpl(schema, tableName, ast, defaultFormat, 'test');
  // oxlint-disable-next-line no-explicit-any
  const query = q as Query<typeof schema, TTable, any>;

  // Enable row count tracking
  runtimeDebugFlags.trackRowCountsVended = true;
  const debug = new Debug();
  delegate.debug = debug;

  try {
    // Execute query
    const start = performance.now();
    await delegate.run(query);
    const duration = performance.now() - start;

    // Collect actual row counts
    const nvisitCounts = debug.getNVisitCounts();
    const rowsScanned = sumRowCounts(nvisitCounts);

    return {duration, rowsScanned};
  } finally {
    // Disable tracking for next iteration
    runtimeDebugFlags.trackRowCountsVended = false;
    delegate.debug = undefined;
  }
}

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

// fix:
// fml bad stats. not flipping user filter.... why....!!
// issueListV2 - roci, assignee=amos
// issueListV2 - roci, assignee=alex
// issueListV2 - roci, creator=clarissa + assignee=holden

// Helper to benchmark planned vs unplanned
async function benchmarkQuery<
  TTable extends keyof typeof schema.tables & string,
>(
  name: string,
  // oxlint-disable-next-line no-explicit-any
  query: Query<typeof schema, TTable, any>,
) {
  // if (count++ !== which) {
  //   return;
  // }
  // if (name !== 'issueListV2 - roci, creator=clarissa + assignee=holden') {
  //   return;
  // }
  // if (name !== 'issueListV2 - roci, creator=clarissa') {
  //   return;
  // }
  if (name !== 'userPickerV2 - roci, creators filter') {
    return;
  }
  // if (name !== 'userPickerV2 - roci, assignees filter') {
  //   return;
  // }
  // userPickerV2 - zero, creators filter
  console.log('\n\n----------------------------------------');
  console.log('RUNNING!', name);
  console.log('----------------------------------------');
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
  // console.log('Planning debug info:');
  console.log(dbg.format());

  // console.log('unplanned asst:', JSON.stringify(unplannedAST, null, 2));
  // console.log('planned ast:', JSON.stringify(plannedClientAST, null, 2));

  const tableName = unplannedAST.table as TTable;

  // Execute unplanned query with row tracking
  const unplannedResult = await executeWithRowTracking(tableName, unplannedAST);
  console.log(
    `Unplanned query took ${unplannedResult.duration.toFixed(2)} ms, scanned ${unplannedResult.rowsScanned} rows`,
  );

  // Execute planned query with row tracking
  const plannedResult = await executeWithRowTracking(
    tableName,
    plannedClientAST,
  );
  console.log(
    `Planned query took ${plannedResult.duration.toFixed(2)} ms, scanned ${plannedResult.rowsScanned} rows`,
  );

  // Calculate speedups
  const timeSpeedup = unplannedResult.duration / plannedResult.duration;
  const rowSpeedup = unplannedResult.rowsScanned / plannedResult.rowsScanned;
  console.log(`Time speedup: ${timeSpeedup.toFixed(2)}x`);
  console.log(`Row scan speedup: ${rowSpeedup.toFixed(2)}x`);

  // Warn if planned was slower
  if (timeSpeedup < 1) {
    const slowdown = plannedResult.duration / unplannedResult.duration;
    console.log('!!!!!!!!!!!!');
    console.warn(
      `Warning: Planned query was slower than unplanned by a factor of ${slowdown.toFixed(
        2,
      )}x`,
    );
    console.log('!!!!!!!!!!!!');
  }

  // Warn if planned scanned more rows
  if (rowSpeedup < 1) {
    const rowSlowdown = plannedResult.rowsScanned / unplannedResult.rowsScanned;
    console.log('!!!!!!!!!!!!');
    console.warn(
      `Warning: Planned query scanned more rows than unplanned by a factor of ${rowSlowdown.toFixed(
        2,
      )}x`,
    );
    console.log('!!!!!!!!!!!!');
  }

  // summary(() => {
  //   bench(`unplanned: ${name}`, async () => {
  //     await delegate.run(unplannedQuery);
  //   });

  //   bench(`planned: ${name}`, async () => {
  //     await delegate.run(plannedQuery);
  //   });
  // });
}

// Benchmark queries from apps/zbugs/shared/queries.ts

// labelsForProject query
await benchmarkQuery(
  'labelsForProject - roci',
  builder.label.whereExists('project', q => q.where('lowerCaseName', 'roci')),
);

// issuePreloadV2 query - simplified version
await benchmarkQuery(
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

// userPickerV2 query - assignees filter
await benchmarkQuery(
  'userPickerV2 - roci, assignees filter',
  builder.user.whereExists('assignedIssues', i =>
    i.whereExists('project', p => p.where('lowerCaseName', 'roci')),
  ),
);

await benchmarkQuery(
  'userPickerV2 - zero, assignees filter',
  builder.user.whereExists('assignedIssues', i =>
    i.whereExists('project', p => p.where('lowerCaseName', 'zero')),
  ),
);

await benchmarkQuery(
  'userPickerV2 - roci, crew filter',
  builder.user.where(({cmp, not, and}) =>
    and(cmp('role', 'crew'), not(cmp('login', 'LIKE', 'rocibot%'))),
  ),
);

await benchmarkQuery(
  'userPickerV2 - zero, crew filter',
  builder.user.where(({cmp, not, and}) =>
    and(cmp('role', 'crew'), not(cmp('login', 'LIKE', 'rocibot%'))),
  ),
);

await benchmarkQuery(
  'userPickerV2 - roci, creators filter',
  builder.user.whereExists('createdIssues', i =>
    i.whereExists('project', p => p.where('lowerCaseName', 'roci')),
  ),
);

await benchmarkQuery(
  'userPickerV2 - zero, creators filter',
  builder.user.whereExists('createdIssues', i =>
    i.whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    }),
  ),
);

await benchmarkQuery(
  'userPickerV2 - disabled, login=clarissa',
  builder.user.where('login', 'clarissa'),
);

// issueDetail query
await benchmarkQuery(
  'issueDetail - roci, by id',
  builder.issue
    .where('id', 'QKAaVd6nYi_dup1')
    .related('project')
    .related('emoji', emoji => emoji.related('creator'))
    .related('creator')
    .related('assignee')
    .related('labels')
    .related('notificationState', q => q.where('userID', 'test-user'))
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

await benchmarkQuery(
  'issueDetail - roci, by shortID',
  builder.issue
    .where('shortID', 777312)
    .related('project')
    .related('emoji', emoji => emoji.related('creator'))
    .related('creator')
    .related('assignee')
    .related('labels')
    .related('notificationState', q => q.where('userID', 'test-user'))
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

await benchmarkQuery(
  'issueDetail - zero, by id',
  builder.issue
    .where('id', 'HVuvDEdnK0TqWF')
    .related('project')
    .related('emoji', emoji => emoji.related('creator'))
    .related('creator')
    .related('assignee')
    .related('labels')
    .related('notificationState', q => q.where('userID', 'test-user'))
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

await benchmarkQuery(
  'issueDetail - zero, by shortID',
  builder.issue
    .where('shortID', 972602)
    .related('project')
    .related('emoji', emoji => emoji.related('creator'))
    .related('creator')
    .related('assignee')
    .related('labels')
    .related('notificationState', q => q.where('userID', 'test-user'))
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

// issueListV2 query - base cases (no filters except project)
await benchmarkQuery(
  'issueListV2 - roci, no filters',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

await benchmarkQuery(
  'issueListV2 - zero, no filters',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'zero'), {
      flip: true,
    })
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

// issueListV2 - creator filters
await benchmarkQuery(
  'issueListV2 - roci, creator=clarissa',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .where(({exists}) => exists('creator', q => q.where('login', 'clarissa')))
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

await benchmarkQuery(
  'issueListV2 - roci, creator=naomi',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .where(({exists}) => exists('creator', q => q.where('login', 'naomi')))
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

// issueListV2 - assignee filters
await benchmarkQuery(
  'issueListV2 - roci, assignee=holden',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .where(({exists}) => exists('assignee', q => q.where('login', 'holden')))
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

await benchmarkQuery(
  'issueListV2 - roci, assignee=alex',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .where(({exists}) => exists('assignee', q => q.where('login', 'alex')))
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

await benchmarkQuery(
  'issueListV2 - roci, assignee=amos',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .where(({exists}) => exists('assignee', q => q.where('login', 'amos')))
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

// issueListV2 - label filters
await benchmarkQuery(
  'issueListV2 - roci, label=bug',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .where(({exists}) => exists('labels', q => q.where('name', 'bug')))
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

await benchmarkQuery(
  'issueListV2 - roci, label=urgent',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .where(({exists}) => exists('labels', q => q.where('name', 'urgent')))
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

await benchmarkQuery(
  'issueListV2 - roci, label=cleanup',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .where(({exists}) => exists('labels', q => q.where('name', 'cleanup')))
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

await benchmarkQuery(
  'issueListV2 - roci, labels=[bug, urgent]',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .where(({exists, and}) =>
      and(
        exists('labels', q => q.where('name', 'bug')),
        exists('labels', q => q.where('name', 'urgent')),
      ),
    )
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

await benchmarkQuery(
  'issueListV2 - roci, labels=[perf, engineering, maintenance]',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .where(({exists, and}) =>
      and(
        exists('labels', q => q.where('name', 'perf')),
        exists('labels', q => q.where('name', 'engineering')),
        exists('labels', q => q.where('name', 'maintenance')),
      ),
    )
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

await benchmarkQuery(
  'issueListV2 - zero, label=bug',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'zero'), {
      flip: true,
    })
    .where(({exists}) => exists('labels', q => q.where('name', 'bug')))
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

// issueListV2 - sorting variations
await benchmarkQuery(
  'issueListV2 - roci, sort by created asc',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('created', 'asc')
    .orderBy('id', 'asc')
    .limit(100),
);

await benchmarkQuery(
  'issueListV2 - roci, sort by created desc',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('created', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

// issueListV2 - complex filter combinations
await benchmarkQuery(
  'issueListV2 - roci, creator=clarissa + assignee=holden',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .where(({exists, and}) =>
      and(
        exists('creator', q => q.where('login', 'clarissa')),
        exists('assignee', q => q.where('login', 'holden')),
      ),
    )
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

await benchmarkQuery(
  'issueListV2 - roci, assignee=holden, label=[armor]',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .where(({exists}) => exists('assignee', q => q.where('login', 'holden')))
    .whereExists('labels', q => q.where('name', 'armor'))
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

await benchmarkQuery(
  'issueListV2 - roci, assignee=alex + labels=[bug, urgent]',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .where(({exists, and}) =>
      and(
        exists('assignee', q => q.where('login', 'alex')),
        exists('labels', q => q.where('name', 'bug')),
        exists('labels', q => q.where('name', 'urgent')),
      ),
    )
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

await benchmarkQuery(
  'issueListV2 - zero, creator=clarissa + label=bug',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'zero'), {
      flip: true,
    })
    .where(({exists, and}) =>
      and(
        exists('creator', q => q.where('login', 'clarissa')),
        exists('labels', q => q.where('name', 'bug')),
      ),
    )
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100),
);

// issueListV2 - pagination
await benchmarkQuery(
  'issueListV2 - roci, limit=50 (first page)',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(50),
);

await benchmarkQuery(
  'issueListV2 - roci, limit=50, start (second page)',
  builder.issue
    .whereExists('project', p => p.where('lowerCaseName', 'roci'), {
      flip: true,
    })
    .related('viewState', q => q.where('userID', 'test-user').one())
    .related('labels')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .start({
      id: 'QKAaVd6nYi_dup1',
      created: 1591810204485.0,
      modified: 1582471103189.0,
    })
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
