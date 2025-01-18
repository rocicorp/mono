import type {LogContext} from '@rocicorp/logger';
import {
  mapPostgresToLite,
  warnIfDataTypeSupported,
} from '../../../../db/pg-to-lite.js';
import {Default} from '../../../../db/postgres-replica-identity-enum.js';
import type {PublishedTableSpec} from '../../../../db/specs.js';
import {ZERO_VERSION_COLUMN_NAME} from '../../../replicator/schema/replication-state.js';
import {unescapedSchema} from './shard.js';

const ALLOWED_IDENTIFIER_CHARS = /^[A-Za-z_]+[A-Za-z0-9_-]*$/;

export function validate(
  lc: LogContext,
  shardID: string,
  table: PublishedTableSpec,
) {
  const shardSchema = unescapedSchema(shardID);
  if (!['public', 'zero', shardSchema].includes(table.schema)) {
    // This may be relaxed in the future. We would need a plan for support in the AST first.
    throw new UnsupportedTableSchemaError(
      'Only the default "public" schema is supported.',
    );
  }
  if (ZERO_VERSION_COLUMN_NAME in table.columns) {
    throw new UnsupportedTableSchemaError(
      `Table "${table.name}" uses reserved column name "${ZERO_VERSION_COLUMN_NAME}"`,
    );
  }
  if (!table.primaryKey?.length && table.replicaIdentity === Default) {
    lc.warn?.(
      `\n\n\n` +
        `Table "${table.name}" needs a primary key in order to be synced to clients. ` +
        `Add one with 'ALTER TABLE "${table.name}" ADD PRIMARY KEY (...)'.` +
        `\n\n\n`,
    );
  }
  if (!ALLOWED_IDENTIFIER_CHARS.test(table.name)) {
    throw new UnsupportedTableSchemaError(
      `Table "${table.name}" has invalid characters.`,
    );
  }
  for (const [col, spec] of Object.entries(mapPostgresToLite(table).columns)) {
    if (!ALLOWED_IDENTIFIER_CHARS.test(col)) {
      throw new UnsupportedTableSchemaError(
        `Column "${col}" in table "${table.name}" has invalid characters.`,
      );
    }
    warnIfDataTypeSupported(lc, spec.dataType, table.name, col);
  }
}

export class UnsupportedTableSchemaError extends Error {
  readonly name = 'UnsupportedTableSchemaError';

  constructor(msg: string) {
    super(msg);
  }
}
