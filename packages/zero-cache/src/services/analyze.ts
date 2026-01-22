import type {LogContext} from '@rocicorp/logger';
import type {AnalyzeQueryResult} from '../../../zero-protocol/src/analyze-query-result.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {PermissionsConfig} from '../../../zero-schema/src/compiled-permissions.ts';
import {Debug} from '../../../zql/src/builder/debug-delegate.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';
import {
  AccumulatorDebugger,
  serializePlanDebugEvents,
} from '../../../zql/src/planner/planner-debug.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {explainQueries} from '../../../zqlite/src/explain-queries.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {TableSource} from '../../../zqlite/src/table-source.ts';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {computeZqlSpecs, mustGetTableSpec} from '../db/lite-tables.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../db/specs.ts';
import {runAst} from './run-ast.ts';
import {TimeSliceTimer, type TokenData} from './view-syncer/view-syncer.ts';
import type {ClientSchema} from '../../../zero-protocol/src/client-schema.ts';

const TIME_SLICE_LAP_THRESHOLD_MS = 200;

export async function analyzeQuery(
  lc: LogContext,
  config: NormalizedZeroConfig,
  clientSchema: ClientSchema,
  ast: AST,
  syncedRows = true,
  vendedRows = false,
  permissions?: PermissionsConfig,
  authData?: TokenData,
  joinPlans = false,
): Promise<AnalyzeQueryResult> {
  using db = new Database(lc, config.replica.file);
  const fullTables = new Map<string, LiteTableSpec>();
  const tableSpecs = new Map<string, LiteAndZqlSpec>();
  const tables = new Map<string, TableSource>();

  computeZqlSpecs(lc, db, tableSpecs, fullTables);

  const planDebugger = joinPlans ? new AccumulatorDebugger() : undefined;
  const costModel = joinPlans
    ? createSQLiteCostModel(db, tableSpecs)
    : undefined;
  const timer = await new TimeSliceTimer(lc).start();
  const shouldYield = () => timer.elapsedLap() > TIME_SLICE_LAP_THRESHOLD_MS;
  const yieldProcess = () => timer.yieldProcess();
  const result = await runAst(
    lc,
    clientSchema,
    ast,
    true,
    {
      applyPermissions: permissions !== undefined,
      syncedRows,
      vendedRows,
      authData,
      db,
      tableSpecs,
      permissions,
      costModel,
      planDebugger,
      host: {
        debug: new Debug(),
        getSource(tableName: string) {
          let source = tables.get(tableName);
          if (source) {
            return source;
          }

          const tableSpec = mustGetTableSpec(tableSpecs, tableName);
          const {primaryKey} = tableSpec.tableSpec;

          source = new TableSource(
            lc,
            config.log,
            db,
            tableName,
            tableSpec.zqlSpec,
            primaryKey,
            shouldYield,
          );
          tables.set(tableName, source);
          return source;
        },
        createStorage() {
          return new MemoryStorage();
        },
        decorateSourceInput: input => input,
        decorateInput: input => input,
        addEdge() {},
        decorateFilterInput: input => input,
      },
    },
    yieldProcess,
  );

  result.sqlitePlans = explainQueries(result.readRowCountsByQuery ?? {}, db);

  if (planDebugger) {
    result.joinPlans = serializePlanDebugEvents(planDebugger.events);
  }

  return result;
}
