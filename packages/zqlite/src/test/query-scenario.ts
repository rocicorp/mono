import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../../zero-cache/src/db/specs.ts';
import {CREATE_TABLE_METADATA_TABLE} from '../../../zero-cache/src/services/replicator/schema/table-metadata.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {buildPipeline} from '../../../zql/src/builder/builder.ts';
import {Debug} from '../../../zql/src/builder/debug-delegate.ts';
import {Catch} from '../../../zql/src/ivm/catch.ts';
import {planQuery} from '../../../zql/src/planner/planner-builder.ts';
import type {ConnectionCostModel} from '../../../zql/src/planner/planner-connection.ts';
import {AccumulatorDebugger} from '../../../zql/src/planner/planner-debug.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import type {SchemaQuery} from '../../../zql/src/query/schema-query.ts';
import {Database} from '../db.ts';
import {createSQLiteCostModel} from '../sqlite-cost-model.ts';
import {newQueryDelegate} from './source-factory.ts';

type QuerySeen = {
  readonly table: string;
  readonly sql: string;
};

class ScenarioDebug extends Debug {
  readonly queries: QuerySeen[] = [];

  override initQuery(table: string, query: string): void {
    if (!this.queries.some(q => q.table === table && q.sql === query)) {
      this.queries.push({table, sql: query});
    }
    super.initQuery(table, query);
  }
}

export type QueryScenario<S extends Schema> = {
  readonly schema: S;
  readonly setup: (db: Database) => void;
  readonly query: (builder: SchemaQuery<S>) => AnyQuery;
};

export type QueryScenarioSQL = {
  readonly table: string;
  readonly sql: string;
};

export type QueryScenarioResult = {
  readonly ast: AST;
  readonly optimizedAST: AST;
  readonly planDebug: string;
  readonly sql: readonly QueryScenarioSQL[];
};

export function runQueryScenario<S extends Schema>(
  scenario: QueryScenario<S>,
): QueryScenarioResult {
  const lc = createSilentLogContext();
  using db = new Database(lc, ':memory:');

  scenario.setup(db);
  db.exec(CREATE_TABLE_METADATA_TABLE);
  db.exec('ANALYZE');

  const tableSpecs = new Map<string, LiteAndZqlSpec>();
  computeZqlSpecs(lc, db, {includeBackfillingColumns: false}, tableSpecs);
  const costModel = createSQLiteCostModel(db, tableSpecs);

  const builder = createBuilder(scenario.schema);
  const ast = asQueryInternals(scenario.query(builder)).ast;
  const planDebugger = new AccumulatorDebugger();
  const optimizedAST = planQueryOnce(ast, costModel, planDebugger);

  const debug = new ScenarioDebug();
  const delegate = newQueryDelegate(lc, testLogConfig, db, scenario.schema);
  delegate.debug = debug;

  const input = buildPipeline(optimizedAST, delegate, 'query-scenario');
  const sink = new Catch(input);
  sink.fetch();
  sink.destroy();

  return {
    ast,
    optimizedAST,
    planDebug: planDebugger.format(),
    sql: debug.queries,
  };
}

function planQueryOnce(
  ast: AST,
  costModel: ConnectionCostModel,
  planDebugger: AccumulatorDebugger,
) {
  // Scenario tests execute this exact optimized AST so fetching does not plan
  // a second time and duplicate planner events.
  return planQuery(ast, costModel, planDebugger);
}
