import type {LogContext} from '@rocicorp/logger';
import type {LogConfig} from '../../../otel/src/log-options.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {
  mapAST,
  type AST,
  type CompoundKey,
} from '../../../zero-protocol/src/ast.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  clientToServer,
  serverToClient,
} from '../../../zero-schema/src/name-mapper.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import type {FilterInput} from '../../../zql/src/ivm/filter-operators.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';
import type {Input} from '../../../zql/src/ivm/operator.ts';
import type {Source, SourceInput} from '../../../zql/src/ivm/source.ts';
import type {SourceFactory} from '../../../zql/src/ivm/test/source-factory.ts';
import type {QueryDelegate} from '../../../zql/src/query/query-delegate.ts';
import {Database} from '../db.ts';
import {compile, sql} from '../internal/sql.ts';
import {TableSource, toSQLiteTypeName} from '../table-source.ts';

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

export function mapResultToClientNames<T, S extends Schema>(
  result: unknown,
  schema: S,
  rootTable: keyof S['tables'] & string,
): T {
  const serverToClientMapper = serverToClient(schema.tables);
  const clientToServerMapper = clientToServer(schema.tables);

  function mapResult(result: unknown, schema: Schema, rootTable: string) {
    // eslint-disable-next-line eqeqeq
    if (result == null) {
      return result;
    }

    if (Array.isArray(result)) {
      return result.map(r => mapResultToClientNames(r, schema, rootTable)) as T;
    }

    const mappedResult: Record<string, unknown> = {};
    const serverTableName = clientToServerMapper.tableName(rootTable);
    for (const [serverCol, v] of Object.entries(result)) {
      if (serverCol === '_0_version') {
        continue;
      }

      try {
        const clientCol = serverToClientMapper.columnName(
          serverTableName,
          serverCol,
        );
        mappedResult[clientCol] = v;
      } catch (e) {
        const relationship = schema.relationships[rootTable][serverCol];
        mappedResult[serverCol] = mapResult(
          v,
          schema,
          (relationship[1] ?? relationship[0]).destSchema,
        );
      }
    }

    return mappedResult as T;
  }

  return mapResult(result, schema, rootTable) as T;
}

export function newQueryDelegate(
  lc: LogContext,
  logConfig: LogConfig,
  db: Database,
  schema: Schema,
): QueryDelegate {
  const sources = new Map<string, Source>();
  const clientToServerMapper = clientToServer(schema.tables);
  const serverToClientMapper = serverToClient(schema.tables);
  return {
    getSource: (serverTableName: string) => {
      const clientTableName = serverToClientMapper.tableName(serverTableName);
      let source = sources.get(serverTableName);
      if (source) {
        return source;
      }

      const tableSchema =
        schema.tables[clientTableName as keyof typeof schema.tables];

      // create the SQLite table
      db.exec(`
      CREATE TABLE IF NOT EXISTS "${serverTableName}" (
        ${Object.entries(tableSchema.columns)
          .map(
            ([name, c]) =>
              `"${clientToServerMapper.columnName(
                clientTableName,
                name,
              )}" ${toSQLiteTypeName(c.type)}`,
          )
          .join(', ')},
        PRIMARY KEY (${tableSchema.primaryKey
          .map(k => `"${clientToServerMapper.columnName(clientTableName, k)}"`)
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
            clientToServerMapper.columnName(clientTableName, k),
            v,
          ]),
        ),
        tableSchema.primaryKey.map(k =>
          clientToServerMapper.columnName(clientTableName, k),
        ) as unknown as CompoundKey,
      );

      sources.set(serverTableName, source);
      return source;
    },

    mapAst(ast: AST): AST {
      return mapAST(ast, clientToServerMapper);
    },

    createStorage() {
      return new MemoryStorage();
    },
    decorateSourceInput(input: SourceInput): Input {
      return input;
    },
    addEdge() {},
    decorateInput(input: Input): Input {
      return input;
    },
    decorateFilterInput(input: FilterInput): FilterInput {
      return input;
    },
    addServerQuery() {
      return () => {};
    },
    addCustomQuery() {
      return () => {};
    },
    updateServerQuery() {},
    updateCustomQuery() {},
    onTransactionCommit() {
      return () => {};
    },
    batchViewUpdates<T>(applyViewUpdates: () => T): T {
      return applyViewUpdates();
    },
    assertValidRunOptions() {},
    flushQueryChanges() {},
    defaultQueryComplete: true,
    addMetric() {},
  };
}
