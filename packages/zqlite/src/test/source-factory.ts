import type {LogContext} from '@rocicorp/logger';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import type {Source} from '../../../zql/src/ivm/source.ts';
import type {SourceFactory} from '../../../zql/src/ivm/test/source-factory.ts';
import {Database} from '../db.ts';
import {compile, sql} from '../internal/sql.ts';
import {TableSource, toSQLiteTypeName} from '../table-source.ts';
import type {LogConfig} from '../../../otel/src/log-options.ts';
import type {QueryDelegate} from '../../../zql/src/query/query-impl.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';

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
  return {
    getSource: (name: string) => {
      let source = sources.get(name);
      if (source) {
        return source;
      }

      const tableSchema = schema.tables[name as keyof typeof schema.tables];

      // create the SQLite table
      db.exec(`
      CREATE TABLE IF NOT EXISTS "${name}" (
        ${Object.entries(tableSchema.columns)
          .map(([name, c]) => `"${name}" ${toSQLiteTypeName(c.type)}`)
          .join(', ')},
        PRIMARY KEY (${tableSchema.primaryKey.map(k => `"${k}"`).join(', ')})
      )`);

      source = new TableSource(
        lc,
        logConfig,
        'query.test.ts',
        db,
        name,
        tableSchema.columns,
        tableSchema.primaryKey,
      );

      sources.set(name, source);
      return source;
    },

    createStorage() {
      return new MemoryStorage();
    },
    addServerQuery() {
      return () => {};
    },
    onTransactionCommit() {
      return () => {};
    },
    batchViewUpdates<T>(applyViewUpdates: () => T): T {
      return applyViewUpdates();
    },
  };
}
