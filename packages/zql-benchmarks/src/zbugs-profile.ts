import {styleText} from 'node:util';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {
  computeZqlSpecs,
  mustGetTableSpec,
} from '../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import {runAst} from '../../zero-cache/src/services/run-ast.ts';
import {clientSchemaFrom} from '../../zero-schema/src/builder/schema-builder.ts';
import {clientToServer} from '../../zero-schema/src/name-mapper.ts';
import type {BuilderDelegate} from '../../zql/src/builder/builder.ts';
import {Debug} from '../../zql/src/builder/debug-delegate.ts';
import {MemoryStorage} from '../../zql/src/ivm/memory-storage.ts';
import {
  AccumulatorDebugger,
  serializePlanDebugEvents,
} from '../../zql/src/planner/planner-debug.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';
import {Database} from '../../zqlite/src/db.ts';
import {explainQueries} from '../../zqlite/src/explain-queries.ts';
import {createSQLiteCostModel} from '../../zqlite/src/sqlite-cost-model.ts';
import {TableSource} from '../../zqlite/src/table-source.ts';
import {builder, schema} from './schema.ts';

/* oxlint-disable no-console */

const dbPath = process.env.ZBUGS_REPLICA_PATH;
if (!dbPath) {
  console.error(
    'ZBUGS_REPLICA_PATH must point at a zbugs replica db (e.g. /Users/mlaw/workspace/tera.db).',
  );
  process.exit(1);
}

const lc = createSilentLogContext();
const db = new Database(lc, dbPath);
db.exec('ANALYZE;');

const tableSpecs = new Map<string, LiteAndZqlSpec>();
computeZqlSpecs(lc, db, {includeBackfillingColumns: false}, tableSpecs);

// Mirror production: planner runs when ZERO_ENABLE_QUERY_PLANNER is set.
// Default to ON here since the whole point is to inspect what the planner does.
const enablePlanner = process.env.ZBUGS_DISABLE_PLANNER ? false : true;
const costModel = enablePlanner
  ? createSQLiteCostModel(db, tableSpecs)
  : undefined;

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
  const planDebugger = new AccumulatorDebugger();
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
      costModel,
      planDebugger,
    },
    async () => {},
  );
  result.sqlitePlans = explainQueries(result.readRowCountsByQuery ?? {}, db);
  result.joinPlans = serializePlanDebugEvents(planDebugger.events);
  return result;
}

function colorTime(durationMs: number): string {
  const text = durationMs.toFixed(2) + 'ms';
  if (durationMs < 100) {
    return styleText('green', text);
  }
  if (durationMs < 1000) {
    return styleText('yellow', text);
  }
  return styleText('red', text);
}

function colorRowsConsidered(n: number): string {
  const text = n.toString();
  if (n < 1000) {
    return styleText('green', text);
  }
  if (n < 10000) {
    return styleText('yellow', text);
  }
  return styleText('red', text);
}

function colorPlanRow(row: string, i: number): string {
  if (row.includes('SCAN')) {
    return i === 0 ? styleText('yellow', row) : styleText('red', row);
  }
  return styleText('green', row);
}

function printResult(
  label: string,
  result: Awaited<ReturnType<typeof runOnce>>,
) {
  console.log(styleText(['blue', 'bold'], `\n=== ${label} ===\n`));
  console.log(styleText('bold', 'planner:'), enablePlanner ? 'ON' : 'OFF');
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

  if (result.joinPlans) {
    console.log(styleText(['blue', 'bold'], '\n--- Planner Decisions ---\n'));
    console.log(result.joinPlans);
  }

  if (result.warnings.length > 0) {
    console.log(styleText(['yellow', 'bold'], '--- Warnings ---'));
    for (const w of result.warnings) console.log(styleText('yellow', w));
  }
}

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
