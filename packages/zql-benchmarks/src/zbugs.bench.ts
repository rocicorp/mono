// oxlint-disable no-console
import {summary} from 'mitata';
import {expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import type {AST, Condition} from '../../zero-protocol/src/ast.ts';
import {type Format} from '../../zql/src/ivm/default-format.ts';
import type {AnyQuery} from '../../zql/src/query/query-builder.ts';
import {QueryImpl} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import {Database} from '../../zqlite/src/db.ts';
import {newQueryDelegate} from '../../zqlite/src/test/source-factory.ts';
import {builder, schema} from './schema.ts';

const dbPath = process.env.ZBUGS_REPLICA_PATH;

if (!dbPath) {
  console.error(
    'Cannot run zbugs.bench.ts without a path to the zbugs replica. Set env var: `ZBUGS_REPLICA_PATH`',
  );
} else {
  // Open the zbugs SQLite database
  const db = new Database(createSilentLogContext(), dbPath);
  const lc = createSilentLogContext();

  // Run ANALYZE to populate SQLite statistics for cost model
  db.exec('ANALYZE;');

  // Get table specs using computeZqlSpecs
  const tableSpecs = new Map<string, LiteAndZqlSpec>();
  computeZqlSpecs(createSilentLogContext(), db, tableSpecs);

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
  function createQuery<TTable extends keyof typeof schema.tables & string>(
    tableName: TTable,
    queryAST: AST,
    format: Format,
  ) {
    return new QueryImpl(
      schema,
      tableName,
      queryAST,
      format,
      'test',
      undefined,
      undefined,
    );
  }

  // Helper to benchmark planned vs unplanned
  async function benchmarkQuery<
    TTable extends keyof typeof schema.tables & string,
  >(_name: string, query: AnyQuery) {
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

    db.exec('BEGIN');
    const start = performance.now();
    await delegate.run(unplannedQuery as AnyQuery);
    const end = performance.now();
    console.log('duration ', end - start);
    db.exec('ROLLBACK');

    summary(() => {
      // bench(`unplanned: ${name}`, async () => {
      //   await delegate.run(unplannedQuery as AnyQuery);
      // });
      // bench(`planned: ${name}`, async () => {
      //   await delegate.run(plannedQuery as AnyQuery);
      // });
    });
  }

  await benchmarkQuery(
    'full issue scan + join',
    builder.issue.related('creator').related('assignee'),
  );

  // run all reads in an explicit tx
  // db.exec('BEGIN');
  // await run();
  // db.exec('ROLLBACK');
}

test('no-op', () => {
  expect(true).toBe(true);
});
