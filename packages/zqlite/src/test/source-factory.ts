import type {LogContext} from '@rocicorp/logger';
import type {LogConfig} from '../../../otel/src/log-options.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';
import type {Input} from '../../../zql/src/ivm/operator.ts';
import type {Source} from '../../../zql/src/ivm/source.ts';
import type {SourceFactory} from '../../../zql/src/ivm/test/source-factory.ts';
import type {QueryDelegate} from '../../../zql/src/query/query-impl.ts';
import {Database} from '../db.ts';
import {compile, sql} from '../internal/sql.ts';
import {TableSource, toSQLiteTypeName} from '../table-source.ts';
import {clientToServer} from '../../../zero-schema/src/name-mapper.ts';
import {mapAST, type AST} from '../../../zero-protocol/src/ast.ts';

export const createSource: SourceFactory = (
  lc: LogContext,
  logConfig: LogConfig,
  tableName: string,
  columns: Record<string, SchemaValue>,
  primaryKey: PrimaryKey,
): Source => {
  const db = new Database(createSilentLogContext(), ':memory:');
  // create a table with desired columns and primary keys
  const query = compile(
    sql`CREATE TABLE ${sql.ident(tableName)} (${sql.join(
      Object.keys(columns).map(c => sql.ident(c)),
      sql`, `,
    )}, PRIMARY KEY (${sql.join(
      primaryKey.map(p => sql.ident(p)),
      sql`, `,
    )}));`,
  );
  db.exec(query);
  return new TableSource(
    lc,
    logConfig,
    'zqlite-test',
    db,
    tableName,
    columns,
    primaryKey,
  );
};

export function newQueryDelegate(
  lc: LogContext,
  logConfig: LogConfig,
  db: Database,
  schema: Schema,
): QueryDelegate {
  const sources = new Map<string, Source>();
  const mapper = clientToServer(schema.tables);
  return {
    getSource: (tableName: string) => {
      const serverTableName = mapper.tableName(tableName);
      let source = sources.get(serverTableName);
      if (source) {
        return source;
      }

      const tableSchema =
        schema.tables[tableName as keyof typeof schema.tables];

      // create the SQLite table
      db.exec(`
      CREATE TABLE IF NOT EXISTS "${serverTableName}" (
        ${Object.entries(tableSchema.columns)
          .map(
            ([name, c]) =>
              `"${mapper.columnName(tableName, name)}" ${toSQLiteTypeName(
                c.type,
              )}`,
          )
          .join(', ')},
        PRIMARY KEY (${tableSchema.primaryKey
          .map(k => `"${mapper.columnName(tableName, k)}"`)
          .join(', ')})
      )`);

      source = new TableSource(
        lc,
        logConfig,
        'query.test.ts',
        db,
        serverTableName,
        Object.fromEntries(
          Object.entries(tableSchema.columns).map(([k, v]) => [
            mapper.columnName(tableName, k),
            v,
          ]),
        ),
        tableSchema.primaryKey,
      );

      sources.set(serverTableName, source);
      return source;
    },

    mapAst(ast: AST): AST {
      return mapAST(ast, mapper);
    },

    createStorage() {
      return new MemoryStorage();
    },
    decorateInput(input: Input): Input {
      return input;
    },
    addServerQuery() {
      return () => {};
    },
    updateServerQuery() {},
    onQueryMaterialized() {},
    onTransactionCommit() {
      return () => {};
    },
    batchViewUpdates<T>(applyViewUpdates: () => T): T {
      return applyViewUpdates();
    },
  };
}
