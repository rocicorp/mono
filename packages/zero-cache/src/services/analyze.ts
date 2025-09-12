import type {LogContext} from '@rocicorp/logger';
import auth from 'basic-auth';
import type {FastifyReply, FastifyRequest} from 'fastify';
import {explainQueries} from '../../../analyze-query/src/explain-queries.ts';
import {runAst} from '../../../analyze-query/src/run-ast.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {Debug} from '../../../zql/src/builder/debug-delegate.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {TableSource} from '../../../zqlite/src/table-source.ts';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {isAdminPasswordValid} from '../config/zero-config.ts';
import {computeZqlSpecs, mustGetTableSpec} from '../db/lite-tables.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../db/specs.ts';

export function setCors(res: FastifyReply) {
  return res
    .header('Access-Control-Allow-Origin', '*')
    .header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
    .header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
    .header('Access-Control-Allow-Credentials', 'true');
}

export async function handleAnalyzeQueryRequest(
  lc: LogContext,
  config: NormalizedZeroConfig,
  req: FastifyRequest,
  res: FastifyReply,
) {
  const credentials = auth(req);
  void setCors(res);
  if (!isAdminPasswordValid(lc, config, credentials?.pass)) {
    await res
      .code(401)
      .header('WWW-Authenticate', 'Basic realm="analyze query Protected Area"')
      .send({unauthorized: true});
    return;
  }

  const body = req.body as {
    ast: AST;
  };

  const db = new Database(lc, config.replica.file);
  const fullTables = new Map<string, LiteTableSpec>();
  const tableSpecs = new Map<string, LiteAndZqlSpec>();
  const tables = new Map<string, TableSource>();

  computeZqlSpecs(lc, db, tableSpecs, fullTables);

  const result = await runAst(lc, body.ast, true, {
    applyPermissions: false,
    outputSyncedRows: true,
    db,
    tableSpecs,
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

  result.plans = explainQueries(result.vendedRowCounts ?? {}, db);

  await res.send(result);
}
