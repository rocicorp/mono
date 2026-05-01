import {styleText} from 'node:util';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {bench, describe} from '../../shared/src/bench.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {
  computeZqlSpecs,
  mustGetTableSpec,
} from '../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import {runAst} from '../../zero-cache/src/services/run-ast.ts';
import type {AST, Condition} from '../../zero-protocol/src/ast.ts';
import {clientSchemaFrom} from '../../zero-schema/src/builder/schema-builder.ts';
import {clientToServer} from '../../zero-schema/src/name-mapper.ts';
import type {BuilderDelegate} from '../../zql/src/builder/builder.ts';
import {Debug} from '../../zql/src/builder/debug-delegate.ts';
import {type Format} from '../../zql/src/ivm/default-format.ts';
import {MemoryStorage} from '../../zql/src/ivm/memory-storage.ts';
import {newQueryImpl} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';
import {Database} from '../../zqlite/src/db.ts';
import {explainQueries} from '../../zqlite/src/explain-queries.ts';
import {TableSource} from '../../zqlite/src/table-source.ts';
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
      builder.issue.related('creator').related('assignee').limit(100),
    );
  });

  // ---------------------------------------------------------------------------
  // Profile mode: when ZBUGS_PROFILE is set, run the suspect zbugs queries
  // once each (not as benchmarks) with full instrumentation, mirroring what
  // `packages/zero-cache/src/services/analyze.ts` does.
  // ---------------------------------------------------------------------------
  if (process.env.ZBUGS_PROFILE) {
    await profileSuspectQueries();
  }

  type ListContextParams = {
    projectName: string;
    open: boolean | null;
    creator: string | null;
    assignee: string | null;
    labels: string[];
    textFilter: string | null;
    sortField: 'modified' | 'created';
    sortDirection: 'asc' | 'desc';
  };

  // Mirror of buildListQuery in apps/zbugs/shared/queries.ts. Permissions
  // collapse to `visibility = 'public'` for an unauthenticated profile run.
  function buildListPageQuery(
    listContext: ListContextParams,
    limit: number,
  ): AnyQuery {
    const {projectName, sortField, sortDirection, open, labels} = listContext;

    let q = builder.issue.related('labels');

    q = q.whereExists(
      'project',
      q => q.where('lowerCaseName', projectName.toLocaleLowerCase()),
      {scalar: true},
    );

    q = q.orderBy(sortField, sortDirection).orderBy('id', sortDirection);
    q = q.limit(limit);

    q = q.where(({and, cmp, exists}) =>
      and(
        // oxlint-disable-next-line eqeqeq
        open != null ? cmp('open', open) : undefined,
        ...labels.map(label =>
          exists('issueLabels', q =>
            q.whereExists(
              'label',
              q =>
                q
                  .where('name', label)
                  .whereExists(
                    'project',
                    q =>
                      q.where('lowerCaseName', projectName.toLocaleLowerCase()),
                    {scalar: true},
                  ),
              {scalar: true},
            ),
          ),
        ),
      ),
    );

    q = q.where('visibility', '=', 'public');

    return q as unknown as AnyQuery;
  }

  async function runOnce(query: AnyQuery) {
    const {ast} = asQueryInternals(query);
    const debug = new Debug();
    const sources = new Map<string, TableSource>();

    const host: BuilderDelegate = {
      debug,
      enableNotExists: true,
      getSource(tableName: string) {
        let s = sources.get(tableName);
        if (s) return s;
        const tableSpec = mustGetTableSpec(tableSpecs, tableName);
        s = new TableSource(
          lc,
          testLogConfig,
          db,
          tableName,
          tableSpec.zqlSpec,
          tableSpec.tableSpec.primaryKey,
        );
        sources.set(tableName, s);
        return s;
      },
      createStorage: () => new MemoryStorage(),
      decorateSourceInput: input => input,
      decorateInput: input => input,
      addEdge() {},
      decorateFilterInput: input => input,
    };

    const clientSchema = clientSchemaFrom(schema).clientSchema;
    const clientToServerMapper = clientToServer(schema.tables);

    const result = await runAst(
      lc,
      clientSchema,
      ast,
      false,
      {
        applyPermissions: false,
        clientToServerMapper,
        syncedRows: false,
        vendedRows: false,
        db,
        tableSpecs,
        host,
      },
      async () => {},
    );
    result.sqlitePlans = explainQueries(result.readRowCountsByQuery ?? {}, db);
    return result;
  }

  function colorTime(durationMs: number): string {
    if (durationMs < 100)
      return styleText('green', durationMs.toFixed(2) + 'ms');
    if (durationMs < 1000)
      return styleText('yellow', durationMs.toFixed(2) + 'ms');
    return styleText('red', durationMs.toFixed(2) + 'ms');
  }

  function colorRowsConsidered(n: number): string {
    if (n < 1000) return styleText('green', n.toString());
    if (n < 10000) return styleText('yellow', n.toString());
    return styleText('red', n.toString());
  }

  function colorPlanRow(row: string, i: number): string {
    if (row.includes('SCAN')) {
      return i === 0 ? styleText('yellow', row) : styleText('red', row);
    }
    return styleText('green', row);
  }

  /* oxlint-disable no-console */
  function printResult(
    label: string,
    result: Awaited<ReturnType<typeof runOnce>>,
  ) {
    console.log(styleText(['blue', 'bold'], `\n=== ${label} ===\n`));
    console.log(styleText('bold', 'total synced rows:'), result.syncedRowCount);

    let totalRowsRead = 0;
    const readCounts = result.readRowCountsByQuery ?? {};
    for (const table of Object.keys(readCounts).sort()) {
      const counts = readCounts[table];
      for (const n of Object.values(counts)) totalRowsRead += n;
      console.log(styleText('bold', `${table} vended:`), counts);
    }
    console.log(
      styleText('bold', 'Rows Read (into JS):'),
      colorRowsConsidered(totalRowsRead),
    );
    console.log(
      styleText('bold', 'time:'),
      colorTime(result.elapsed ?? result.end - result.start),
    );

    const dbScans = result.dbScansByQuery ?? {};
    let totalNVisit = 0;
    console.log(
      styleText(['blue', 'bold'], '\n--- Rows Scanned (by SQLite) ---\n'),
    );
    for (const [table, queries] of Object.entries(dbScans)) {
      console.log(styleText('bold', `${table}:`), queries);
      for (const c of Object.values(queries)) totalNVisit += c;
    }
    console.log(
      styleText('bold', 'total rows scanned:'),
      colorRowsConsidered(totalNVisit),
    );

    const plans = result.sqlitePlans ?? {};
    console.log(styleText(['blue', 'bold'], '\n--- Query Plans ---\n'));
    for (const [q, plan] of Object.entries(plans)) {
      console.log(styleText('bold', 'query'), q);
      console.log(plan.map((row, i) => colorPlanRow(row, i)).join('\n'));
      console.log('');
    }

    if (result.warnings.length > 0) {
      console.log(styleText(['yellow', 'bold'], '--- Warnings ---'));
      for (const w of result.warnings) console.log(styleText('yellow', w));
    }
  }

  async function profileSuspectQueries() {
    const baseListContext: ListContextParams = {
      projectName: 'gatewaycore',
      sortField: 'modified',
      sortDirection: 'desc',
      open: true,
      creator: null,
      assignee: null,
      labels: [],
      textFilter: null,
    };
    const limit = 50;

    const variants: {label: string; ctx: ListContextParams}[] = [
      {
        label: 'issueListV2 — gatewaycore + label=api-gateway (suspect)',
        ctx: {...baseListContext, labels: ['api-gateway']},
      },
      {
        label: 'issueListV2 — gatewaycore (no label, baseline)',
        ctx: baseListContext,
      },
      {
        label: 'issueListV2 — gatewaycore + label=enhancement (other label)',
        ctx: {...baseListContext, labels: ['enhancement']},
      },
    ];

    for (const {label, ctx} of variants) {
      const query = buildListPageQuery(ctx, limit);
      console.log(styleText(['cyan', 'bold'], `\n>>> ${label}`));
      try {
        const result = await runOnce(query);
        printResult(label, result);
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        console.log(styleText('red', `failed: ${msg}`));
      }
    }
  }
  /* oxlint-enable no-console */
}
