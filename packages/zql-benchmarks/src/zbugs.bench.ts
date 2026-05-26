import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {bench, describe} from '../../shared/src/bench.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import type {AST, Condition} from '../../zero-protocol/src/ast.ts';
import {type Format} from '../../zql/src/ivm/default-format.ts';
import {newQueryImpl} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';
import {Database} from '../../zqlite/src/db.ts';
import {newQueryDelegate} from '../../zqlite/src/test/source-factory.ts';
import {builder, schema} from './schema.ts';

const dbPath = process.env.ZBUGS_REPLICA_PATH;

if (!dbPath) {
  // oxlint-disable-next-line no-console
  console.error(
    'Cannot run zbugs.bench.ts without a path to the zbugs replica. Set env var: `ZBUGS_REPLICA_PATH`',
  );
  bench.skip('skipped - no ZBUGS_REPLICA_PATH', () => {
    // This callback is intentionally non-empty to satisfy static analysis tools.
    // It will never be executed because the benchmark is marked as skipped.
  });
} else {
  // Open the zbugs SQLite database
  const db = new Database(createSilentLogContext(), dbPath);
  const lc = createSilentLogContext();

  // Run ANALYZE to populate SQLite statistics for cost model
  db.exec('ANALYZE;');

  // Get table specs using computeZqlSpecs
  const tableSpecs = new Map<string, LiteAndZqlSpec>();
  computeZqlSpecs(
    createSilentLogContext(),
    db,
    {includeBackfillingColumns: false},
    tableSpecs,
  );

  // Create SQLite cost model
  // const costModel = createSQLiteCostModel(db, tableSpecs);
  // const clientToServerMapper = clientToServer(schema.tables);
  // const serverToClientMapper = serverToClient(schema.tables);

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
  function createQuery(
    tableName: string,
    queryAST: AST,
    format: Format,
  ): AnyQuery {
    return newQueryImpl(
      schema,
      tableName as keyof typeof schema.tables,
      queryAST,
      format,
      'test',
    );
  }

  // Helper to benchmark planned vs unplanned
  function registerBenchmark<TTable extends keyof typeof schema.tables>(
    name: string,
    query: AnyQuery,
  ) {
    const unplannedAST = asQueryInternals(query).ast;
    const format = asQueryInternals(query).format;

    // const mappedAST = mapAST(unplannedAST, clientToServerMapper);
    // const mappedASTCopy = setFlipToFalseInAST(mappedAST);
    // const dbg = new AccumulatorDebugger();
    // const plannedServerAST = planQuery(mappedASTCopy, costModel, dbg);
    // const plannedClientAST = mapAST(plannedServerAST, serverToClientMapper);
    // const plannedQuery = createQuery(tableName, plannedClientAST);

    const tableName = unplannedAST.table as TTable;
    const unplannedQuery = createQuery(tableName, unplannedAST, format);

    bench(name, async () => {
      await delegate.run(unplannedQuery as AnyQuery);
    });
  }

  describe('zbugs', () => {
    registerBenchmark(
      'full issue scan + join',
      builder.issue.related('creator').related('assignee'),
    );
  });
}
