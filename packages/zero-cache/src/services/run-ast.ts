import type {LogContext} from '@rocicorp/logger';
// @circular-dep-ignore
import {astToZQL} from '../../../ast-to-zql/src/ast-to-zql.ts';
// @circular-dep-ignore
import {formatOutput} from '../../../ast-to-zql/src/format.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import type {AnalyzeQueryResult} from '../../../zero-protocol/src/analyze-query-result.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {mapAST} from '../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {hashOfAST} from '../../../zero-protocol/src/query-hash.ts';
import type {PermissionsConfig} from '../../../zero-schema/src/compiled-permissions.ts';
import type {NameMapper} from '../../../zero-schema/src/name-mapper.ts';
import {
  buildPipeline,
  type BuilderDelegate,
} from '../../../zql/src/builder/builder.ts';
import type {ConnectionCostModel} from '../../../zql/src/planner/planner-connection.ts';
import type {PlanDebugger} from '../../../zql/src/planner/planner-debug.ts';
import type {Database} from '../../../zqlite/src/db.ts';
import type {JWTAuth} from '../auth/auth.ts';
import {transformAndHashQuery} from '../auth/read-authorizer.ts';
import type {LiteAndZqlSpec} from '../db/specs.ts';
import {hydrate} from './view-syncer/pipeline-driver.ts';

export type RunAstOptions = {
  applyPermissions?: boolean | undefined;
  auth?: JWTAuth | undefined;
  clientToServerMapper?: NameMapper | undefined;
  costModel?: ConnectionCostModel | undefined;
  db: Database;
  host: BuilderDelegate;
  permissions?: PermissionsConfig | undefined;
  planDebugger?: PlanDebugger | undefined;
  syncedRows?: boolean | undefined;
  tableSpecs: Map<string, LiteAndZqlSpec>;
  vendedRows?: boolean | undefined;
};

export async function runAst(
  lc: LogContext,
  clientSchema: ClientSchema,
  ast: AST,
  isTransformed: boolean,
  options: RunAstOptions,
  yieldProcess: () => Promise<void>,
): Promise<AnalyzeQueryResult> {
  const {clientToServerMapper, permissions, host} = options;
  const result: AnalyzeQueryResult = {
    warnings: [],
    syncedRows: undefined,
    syncedRowCount: 0,
    start: 0,
    end: 0,
    elapsed: 0,
    afterPermissions: undefined,
    readRows: undefined,
    readRowCountsByQuery: {},
    readRowCount: undefined,
  };

  if (!isTransformed) {
    // map the AST to server names if not already transformed
    ast = mapAST(ast, must(clientToServerMapper));
  }
  if (options.applyPermissions) {
    const auth = options.auth;
    if (!auth) {
      result.warnings.push(
        'No auth data provided. Permission rules will compare to `NULL` wherever an auth data field is referenced.',
      );
    }
    ast = transformAndHashQuery(
      lc,
      'clientGroupIDForAnalyze',
      ast,
      must(permissions),
      auth,
      false,
    ).transformedAst;
    result.afterPermissions = await formatOutput(ast.table + astToZQL(ast));
  }

  const pipeline = buildPipeline(
    ast,
    host,
    'query-id',
    options.costModel,
    lc,
    options.planDebugger,
  );

  const start = performance.now();

  let syncedRowCount = 0;
  const rowsByTable: Record<string, Row[]> = {};
  const seenByTable: Set<string> = new Set();
  for (const rowChange of hydrate(pipeline, hashOfAST(ast), clientSchema)) {
    if (rowChange === 'yield') {
      await yieldProcess();
      continue;
    }
    assert(rowChange.type === 'add', 'Hydration only handles add row changes');

    // yield to other tasks to avoid blocking for too long
    if (syncedRowCount % 10 === 0) {
      await Promise.resolve();
    }
    if (syncedRowCount % 100 === 0) {
      await sleep(1);
    }

    let rows: Row[] = rowsByTable[rowChange.table];
    const s = rowChange.table + '.' + JSON.stringify(rowChange.row);
    if (seenByTable.has(s)) {
      continue; // skip duplicates
    }
    syncedRowCount++;
    seenByTable.add(s);
    if (options.syncedRows) {
      if (!rows) {
        rows = [];
        rowsByTable[rowChange.table] = rows;
      }
      rows.push(rowChange.row);
    }
  }

  const end = performance.now();
  if (options.syncedRows) {
    result.syncedRows = rowsByTable;
  }
  result.start = start;
  result.end = end;
  result.elapsed = end - start;

  // Always include the count of synced and vended rows.
  result.syncedRowCount = syncedRowCount;
  result.readRowCountsByQuery = host.debug?.getVendedRowCounts() ?? {};
  let readRowCount = 0;
  for (const c of Object.values(result.readRowCountsByQuery)) {
    for (const v of Object.values(c)) {
      readRowCount += v;
    }
  }
  result.readRowCount = readRowCount;
  result.dbScansByQuery = host.debug?.getNVisitCounts() ?? {};

  if (options.vendedRows) {
    result.readRows = host.debug?.getVendedRows();
  }
  return result;
}
