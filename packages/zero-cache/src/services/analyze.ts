import type {LogContext} from '@rocicorp/logger';
import type {AnalyzeQueryResult} from '../../../zero-protocol/src/analyze-query-result.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {PermissionsConfig} from '../../../zero-schema/src/compiled-permissions.ts';
import {Debug} from '../../../zql/src/builder/debug-delegate.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {explainQueries} from '../../../zqlite/src/explain-queries.ts';
import {TableSource} from '../../../zqlite/src/table-source.ts';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {computeZqlSpecs, mustGetTableSpec} from '../db/lite-tables.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../db/specs.ts';
import {runAst} from './run-ast.ts';
import type {TokenData} from './view-syncer/view-syncer.ts';
import type {ClientSchema} from '../../../zero-protocol/src/client-schema.ts';

export async function analyzeQuery(
  lc: LogContext,
  config: NormalizedZeroConfig,
  clientSchema: ClientSchema,
  ast: AST,
  syncedRows = true,
  vendedRows = false,
  permissions?: PermissionsConfig,
  authData?: TokenData,
): Promise<AnalyzeQueryResult> {
  using db = new Database(lc, config.replica.file);
  const fullTables = new Map<string, LiteTableSpec>();
  const tableSpecs = new Map<string, LiteAndZqlSpec>();
  const tables = new Map<string, TableSource>();

  computeZqlSpecs(lc, db, tableSpecs, fullTables);

  const result = await runAst(lc, clientSchema, ast, true, {
    applyPermissions: permissions !== undefined,
    syncedRows,
    vendedRows,
    authData,
    db,
    tableSpecs,
    permissions,
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
  });

  result.plans = explainQueries(result.readRowCountsByQuery ?? {}, db);
  return result;
}
