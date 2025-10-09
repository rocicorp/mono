import {LogContext} from '@rocicorp/logger';
import {astToZQL} from '../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../ast-to-zql/src/format.ts';
import {assert} from '../../shared/src/asserts.ts';
import {must} from '../../shared/src/must.ts';
import {sleep} from '../../shared/src/sleep.ts';
import {transformAndHashQuery} from '../../zero-cache/src/auth/read-authorizer.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import {hydrate} from '../../zero-cache/src/services/view-syncer/pipeline-driver.ts';
import type {AnalyzeQueryResult} from '../../zero-protocol/src/analyze-query-result.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import {mapAST} from '../../zero-protocol/src/ast.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {hashOfAST} from '../../zero-protocol/src/query-hash.ts';
import type {PermissionsConfig} from '../../zero-schema/src/compiled-permissions.ts';
import type {NameMapper} from '../../zero-schema/src/name-mapper.ts';
import {
  buildPipeline,
  type BuilderDelegate,
} from '../../zql/src/builder/builder.ts';
import type {Database} from '../../zqlite/src/db.ts';

export type RunAstOptions = {
  applyPermissions?: boolean | undefined;
  authData?: string | undefined;
  clientToServerMapper?: NameMapper | undefined;
  db: Database;
  host: BuilderDelegate;
  permissions?: PermissionsConfig | undefined;
  syncedRows?: boolean | undefined;
  tableSpecs: Map<string, LiteAndZqlSpec>;
  vendedRows?: boolean | undefined;
};

export async function runAst(
  lc: LogContext,
  ast: AST,
  isTransformed: boolean,
  options: RunAstOptions,
): Promise<AnalyzeQueryResult> {
  const {clientToServerMapper, permissions, host, db} = options;
  const result: AnalyzeQueryResult = {
    warnings: [],
    syncedRows: undefined,
    syncedRowCount: 0,
    start: 0,
    end: 0,
    afterPermissions: undefined,
    vendedRowCounts: {},
    vendedRows: undefined,
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
      must(permissions),
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
  const seenByTable: Set<string> = new Set();
  for (const rowChange of hydrate(pipeline, hashOfAST(ast), tableSpecs)) {
    assert(rowChange.type === 'add');

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

  // Always include the count of synced and vended rows.
  result.syncedRowCount = syncedRowCount;
  result.vendedRowCounts = host.debug?.getVendedRowCounts() ?? {};

  if (options.vendedRows) {
    result.vendedRows = host.debug?.getVendedRows();
  }
  return result;
}
