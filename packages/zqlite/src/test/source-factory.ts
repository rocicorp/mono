import {Database} from '../db.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';
import type {Source} from '../../../zql/src/ivm/source.js';
import type {SourceFactory} from '../../../zql/src/ivm/test/source-factory.js';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.js';
import {compile, sql} from '../internal/sql.js';
import {TableSource, toSQLiteTypeName} from '../table-source.js';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import type {QueryDelegate} from '../../../zql/src/query/query-impl.js';
import {normalizeTableSchema} from '../../../zero-schema/src/normalize-table-schema.js';
import type {Schema} from '../../../zero-schema/src/schema.js';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.js';

export const createSource: SourceFactory = (
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
  return new TableSource('zqlite-test', db, tableName, columns, primaryKey);
};

export function createQueryDelegate(fullSchema: Schema): QueryDelegate {
  const db = new Database(createSilentLogContext(), ':memory:');
  const sources = new Map<string, Source>();
  return {
    getSource: (name: string) => {
      let source = sources.get(name);
      if (source) {
        return source;
      }
      const schema = normalizeTableSchema(fullSchema.tables[name]);

      // create the SQLite table
      db.exec(`
      CREATE TABLE "${name}" (
        ${Object.entries(schema.columns)
          .map(([name, c]) => `"${name}" ${toSQLiteTypeName(c.type)}`)
          .join(', ')},
        PRIMARY KEY (${schema.primaryKey.map(k => `"${k}"`).join(', ')})
      )`);

      source = new TableSource(
        'query.test.ts',
        db,
        name,
        schema.columns,
        schema.primaryKey,
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
