import type {LogContext} from '@rocicorp/logger';
import {warnIfDataTypeSupported} from '../../../../db/pg-to-lite.js';
import type {TableSpec} from '../../../../db/specs.js';
import {ZERO_VERSION_COLUMN_NAME} from '../../../replicator/schema/replication-state.js';
import {unescapedSchema} from './shard.js';

const ALLOWED_IDENTIFIER_CHARS = /^[A-Za-z_]+[A-Za-z0-9_-]*$/;

export function validate(lc: LogContext, shardID: string, table: TableSpec) {
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
  if (table.primaryKey.length === 0) {
    throw new UnsupportedTableSchemaError(
      `Table "${table.name}" does not have a PRIMARY KEY`,
    );
  }
  if (!ALLOWED_IDENTIFIER_CHARS.test(table.name)) {
    throw new UnsupportedTableSchemaError(
      `Table "${table.name}" has invalid characters.`,
    );
  }
  for (const [col, spec] of Object.entries(table.columns)) {
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
