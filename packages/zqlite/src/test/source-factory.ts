import {Database} from '../db.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';
import type {Source} from '../../../zql/src/ivm/source.js';
import type {SourceFactory} from '../../../zql/src/ivm/test/source-factory.js';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.js';
import {compile, sql} from '../internal/sql.js';
import {TableSource} from '../table-source.js';

export const createSource: SourceFactory = (
  tableName: string,
  columns: Record<string, SchemaValue>,
  primaryKey: readonly [string, ...string[]],
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
  return new TableSource(db, tableName, columns, primaryKey);
};
