/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import auth from 'basic-auth';
import type {FastifyReply, FastifyRequest} from 'fastify';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import type {LogContext} from '@rocicorp/logger';
import {runAst} from '../../../analyze-query/src/run-ast.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {Debug} from '../../../zql/src/builder/debug-delegate.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../db/specs.ts';
import {computeZqlSpecs, mustGetTableSpec} from '../db/lite-tables.ts';
import {TableSource} from '../../../zqlite/src/table-source.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';
import {explainQueries} from '../../../analyze-query/src/explain-queries.ts';

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
  const expectedPassword = config.adminPassword;
  void setCors(res);
  if (!expectedPassword || credentials?.pass !== expectedPassword) {
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
