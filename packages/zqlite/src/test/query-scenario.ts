import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../../zero-cache/src/db/specs.ts';
import {CREATE_TABLE_METADATA_TABLE} from '../../../zero-cache/src/services/replicator/schema/table-metadata.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
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
  table: string;
  sql: string;
  calls: number;
};

class ScenarioDebug extends Debug {
  readonly queries: QuerySeen[] = [];

  override initQuery(table: string, query: string): void {
    const seen = this.queries.find(q => q.table === table && q.sql === query);
    if (seen) {
      seen.calls++;
    } else {
      this.queries.push({table, sql: query, calls: 1});
    }
    super.initQuery(table, query);
  }

  compactQueries(): readonly QueryScenarioSQL[] {
    return this.queries.map(({table, sql, calls}) =>
      calls === 1 ? {table, sql} : {table, sql, calls},
    );
  }
}

export type QueryScenario<S extends Schema> = {
  readonly name: string;
  readonly schema: S;
  readonly seed: (db: Database) => void;
  readonly query: (builder: SchemaQuery<S>) => AnyQuery;
  readonly expectations: QueryScenarioExpectations;
  readonly knownFailure?: QueryScenarioKnownFailure;
};

export type QueryScenarioKnownFailure = {
  readonly reason: string;
  readonly current: string;
  readonly desired: string;
  readonly engineIdea: string;
} & (
  | {readonly currentSQL: readonly QueryScenarioSQL[]}
  | {readonly currentError: string}
);

export type QueryScenarioExpectations = {
  readonly optimizedAST?: object;
  readonly planDebug?: readonly string[];
  readonly sql?: readonly QueryScenarioSQL[];
  readonly rows?: readonly Row[];
};

export type QueryScenarioSQL = {
  readonly table: string;
  readonly sql: string;
  readonly calls?: number | undefined;
};

export type QueryScenarioResult = {
  readonly ast: AST;
  readonly optimizedAST: AST;
  readonly planDebug: string;
  readonly sql: readonly QueryScenarioSQL[];
  readonly rows: readonly Row[];
};

export function runQueryScenario<S extends Schema>(
  scenario: QueryScenario<S>,
): QueryScenarioResult {
  const lc = createSilentLogContext();
  using db = new Database(lc, ':memory:');

  scenario.seed(db);
  db.exec(CREATE_TABLE_METADATA_TABLE);
  db.exec('ANALYZE');

  const tableSpecs = new Map<string, LiteAndZqlSpec>();
  computeZqlSpecs(lc, db, {includeBackfillingColumns: false}, tableSpecs);
  const costModel = createSQLiteCostModel(db, tableSpecs);

  const builder = createBuilder(scenario.schema);
  const ast = asQueryInternals(scenario.query(builder)).ast;
  const optimizedAST = planQueryOnce(ast, costModel, new AccumulatorDebugger());

  const debug = new ScenarioDebug();
  const delegate = newQueryDelegate(lc, testLogConfig, db, scenario.schema);
  delegate.debug = debug;

  const planDebugger = new AccumulatorDebugger();
  const input = buildPipeline(
    ast,
    delegate,
    'query-scenario',
    costModel,
    lc,
    planDebugger,
  );
  const sink = new Catch(input);
  // SQL shape alone can be misleading because repeated query text can hide
  // multiple physical scans with different bind values. ScenarioDebug keeps the
  // SQL list readable by compacting repeated text into a calls count, so the
  // intersection scenarios can prove both membership scans happen.
  const rows = sink
    .fetch()
    .filter(node => node !== 'yield')
    .map(node => node.row);
  sink.destroy();

  return {
    ast,
    optimizedAST,
    planDebug: planDebugger.format(),
    sql: debug.compactQueries(),
    rows,
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
