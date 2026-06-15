import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../../zero-cache/src/db/specs.ts';
import {CREATE_TABLE_METADATA_TABLE} from '../../../zero-cache/src/services/replicator/schema/table-metadata.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {buildPipeline} from '../../../zql/src/builder/builder.ts';
import {Debug} from '../../../zql/src/builder/debug-delegate.ts';
import {Catch} from '../../../zql/src/ivm/catch.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import type {SchemaQuery} from '../../../zql/src/query/schema-query.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {newQueryDelegate} from '../../../zqlite/src/test/source-factory.ts';
import {digestRows} from './digest.ts';

type QuerySeen = {
  table: string;
  sql: string;
  calls: number;
};

class ScenarioDebug extends Debug {
  readonly #queries: QuerySeen[] = [];

  override initQuery(table: string, query: string): void {
    const seen = this.#queries.find(
      candidate => candidate.table === table && candidate.sql === query,
    );
    if (seen) {
      seen.calls++;
    } else {
      this.#queries.push({table, sql: query, calls: 1});
    }
    super.initQuery(table, query);
  }

  compactQueries(): readonly QueryScenarioSQL[] {
    return this.#queries.map(({table, sql, calls}) =>
      calls === 1 ? {table, sql} : {table, sql, calls},
    );
  }
}

export type QueryScenario<S extends Schema = Schema> = {
  readonly name: string;
  readonly schema: S;
  readonly seed: (db: Database) => void;
  readonly query: (builder: SchemaQuery<S>) => AnyQuery;
};

export type QueryScenarioSQL = {
  readonly table: string;
  readonly sql: string;
  readonly calls?: number | undefined;
};

export type QueryScenarioResult = {
  readonly sql: readonly QueryScenarioSQL[];
  readonly rows: readonly Row[];
  readonly rowDigest: string;
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

  const debug = new ScenarioDebug();
  const delegate = newQueryDelegate(lc, testLogConfig, db, scenario.schema);
  delegate.debug = debug;

  const input = buildPipeline(ast, delegate, 'query-scenario', costModel, lc);
  const sink = new Catch(input);
  const rows = sink
    .fetch()
    .filter(node => node !== 'yield')
    .map(node => node.row);
  sink.destroy();

  return {
    sql: debug.compactQueries(),
    rows,
    rowDigest: digestRows(rows),
  };
}
