import auth from 'basic-auth';
import type {FastifyReply, FastifyRequest} from 'fastify';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import type {LogContext} from '@rocicorp/logger';
import {runAst} from '../../../analyze-query/src/run-ast.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {Debug} from '../../../zql/src/builder/debug-delegate.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../db/specs.ts';
import {computeZqlSpecs} from '../db/lite-tables.ts';
import {TableSource} from '../../../zqlite/src/table-source.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';

export async function handleAnalyzeQueryRequest(
  lc: LogContext,
  config: NormalizedZeroConfig,
  req: FastifyRequest,
  res: FastifyReply,
) {
  const credentials = auth(req);
  const expectedPassword = config.adminPassword;
  if (!expectedPassword || credentials?.pass !== expectedPassword) {
    void res
      .code(401)
      .header('WWW-Authenticate', 'Basic realm="analyze query Protected Area"')
      .send('Unauthorized');
  }

  const body = req.body as {
    ast: AST;
  };

  console.log('XCX got request', body);

  const db = new Database(lc, config.replica.file);
  const fullTables = new Map<string, LiteTableSpec>();
  const tableSpecs = new Map<string, LiteAndZqlSpec>();
  const tables = new Map<string, TableSource>();

  computeZqlSpecs(lc, db, tableSpecs, fullTables);

  const result = await runAst(lc, body.ast, true, {
    applyPermissions: false,
    outputSyncedRows: true,
    db,
    host: {
      debug: new Debug(),
      getSource(tableName: string) {
        let source = tables.get(tableName);
        if (source) {
          return source;
        }

        const tableSpec = tableSpecs.get(tableName);
        if (!tableSpec) {
          throw new Error(
            `table '${tableName}' is not one of: ${[...tableSpecs.keys()]
              .filter(t => !t.includes('.') && !t.startsWith('_litestream_'))
              .sort()}. ` +
              `Check the spelling and ensure that the table has a primary key.`,
          );
        }
        const {primaryKey} = tableSpec.tableSpec;
        assert(primaryKey?.length);

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

  console.log('XCX sending result', result);

  await res.send(result);
}
