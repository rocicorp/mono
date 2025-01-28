import type {LogContext} from '@rocicorp/logger';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import type {Source} from '../../../zql/src/ivm/source.ts';
import type {SourceFactory} from '../../../zql/src/ivm/test/source-factory.ts';
import {Database} from '../db.ts';
import {compile, sql} from '../internal/sql.ts';
import {TableSource} from '../table-source.ts';
import type {LogConfig} from '../../../otel/src/log-options.ts';

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
