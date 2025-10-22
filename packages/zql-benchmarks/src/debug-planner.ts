import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {createSQLiteCostModel} from '../../zqlite/src/sqlite-cost-model.ts';
import {clientToServer} from '../../zero-schema/src/name-mapper.ts';
import {buildPlanGraph} from '../../zql/src/planner/planner-builder.ts';
import {mapAST} from '../../zero-protocol/src/ast.ts';
import {AccumulatorDebugger} from '../../zql/src/planner/planner-debug.ts';

const pgContent = await getChinook();

const {dbs, queries} = await bootstrap({
  suiteName: 'debug_planner',
  zqlSchema: schema,
  pgContent,
});

// Run ANALYZE to populate SQLite statistics for cost model
dbs.sqlite.exec('ANALYZE;');

// Create SQLite cost model
const costModel = createSQLiteCostModel(
  dbs.sqlite,
  Object.fromEntries(
    Object.entries(schema.tables).map(([k, v]) => [
      'serverName' in v ? v.serverName : k,
      {
        columns: Object.fromEntries(
          Object.entries(v.columns).map(([colName, col]) => [
            'serverName' in col ? col.serverName : colName,
            {
              ...col,
            },
          ]),
        ),
        primaryKey: v.primaryKey,
      },
    ]),
  ),
);

// Create name mapper
const clientToServerMapper = clientToServer(schema.tables);

// Get the query
const query = queries.sqlite.track.whereExists('playlists');
const unplannedAST = query.ast;

// Map to server names
const mappedAST = mapAST(unplannedAST, clientToServerMapper);

// Build the plan graph
const plans = buildPlanGraph(mappedAST, costModel);

// Create debugger and plan with it
const dbg = new AccumulatorDebugger();
plans.plan.plan(dbg);

// Print debug output
console.log('\n========================================');
console.log('Planning: track.exists(playlists)');
console.log('========================================\n');
console.log(dbg.format());
console.log('\n========================================\n');

process.exit(0);
