import type {AST} from '../../zero-protocol/src/ast.ts';
import {buildPipeline} from '../../zql/src/builder/builder.ts';
import {hydrate} from '../../zero-cache/src/services/view-syncer/pipeline-driver.ts';
import {transformAndHashQuery} from '../../zero-cache/src/auth/read-authorizer.ts';
import {assert} from '../../shared/src/asserts.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {hashOfAST} from '../../zero-protocol/src/query-hash.ts';
import {mapAST} from '../../zero-protocol/src/ast.ts';
import type {NameMapper} from '../../zero-schema/src/name-mapper.ts';
import {LogContext} from '@rocicorp/logger';
import type {PermissionsConfig} from '../../zero-schema/src/compiled-permissions.ts';
import {astToZQL} from '../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../ast-to-zql/src/format.ts';
import type {Database} from '../../zqlite/src/db.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';
import {must} from '../../shared/src/must.ts';
import type {
  RowCountsBySource,
  RowsBySource,
} from '../../zql/src/builder/debug-delegate.ts';

export type RunResult = {
  warnings: string[];
  syncedRows: Record<string, Row[]>;
  syncedRowCount: number;
  start: number;
  end: number;
  afterPermissions: string | undefined;
  vendedRowCounts: RowCountsBySource | undefined;
  vendedRows: RowsBySource | undefined;
};

export async function runAst(
  lc: LogContext,
  ast: AST,
  isTransformed: boolean,
  options: {
    applyPermissions: boolean;
    authData: string | undefined;
    clientToServerMapper: NameMapper | undefined;
    permissions: PermissionsConfig;
    outputSyncedRows: boolean;
    db: Database;
    host: QueryDelegate;
  },
): Promise<RunResult> {
  const {clientToServerMapper, permissions, host, db} = options;
  const result: RunResult = {
    warnings: [],
    syncedRows: {},
    syncedRowCount: 0,
    start: 0,
    end: 0,
    afterPermissions: undefined,
    vendedRowCounts: host.debug?.getVendedRowCounts(),
    vendedRows: host.debug?.getVendedRows(),
  };

  if (!isTransformed) {
    // map the AST to server names if not already transformed
    ast = mapAST(ast, must(clientToServerMapper));
  }
  if (options.applyPermissions) {
    const authData = options.authData ? JSON.parse(options.authData) : {};
    if (!options.authData) {
      result.warnings.push(
        'No auth data provided. Permission rules will compare to `NULL` wherever an auth data field is referenced.',
      );
    }
    ast = transformAndHashQuery(
      lc,
      'clientGroupIDForAnalyze',
      ast,
      permissions,
      authData,
      false,
    ).transformedAst;
    result.afterPermissions = await formatOutput(ast.table + astToZQL(ast));
  }

  const tableSpecs = computeZqlSpecs(lc, db);
  const pipeline = buildPipeline(ast, host, 'query-id');

  const start = performance.now();

  let syncedRowCount = 0;
  const rowsByTable: Record<string, Row[]> = {};
  for (const rowChange of hydrate(pipeline, hashOfAST(ast), tableSpecs)) {
    assert(rowChange.type === 'add');
    syncedRowCount++;
    if (options.outputSyncedRows) {
      let rows: Row[] = rowsByTable[rowChange.table];
      if (!rows) {
        rows = [];
        rowsByTable[rowChange.table] = rows;
      }
      rows.push(rowChange.row);
    }
  }

  const end = performance.now();
  result.syncedRows = rowsByTable;
  result.start = start;
  result.end = end;
  result.syncedRowCount = syncedRowCount;
  result.vendedRowCounts = host.debug?.getVendedRowCounts();
  result.vendedRows = host.debug?.getVendedRows();
  return result;
}
