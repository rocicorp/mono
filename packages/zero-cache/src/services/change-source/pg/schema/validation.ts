/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import type {LogContext} from '@rocicorp/logger';
import {
  mapPostgresToLite,
  warnIfDataTypeSupported,
} from '../../../../db/pg-to-lite.ts';
import {
  Default,
  Index,
  Nothing,
} from '../../../../db/postgres-replica-identity-enum.ts';
import type {
  PublishedIndexSpec,
  PublishedTableSpec,
} from '../../../../db/specs.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../../../replicator/schema/replication-state.ts';

export const ALLOWED_APP_ID_CHARACTERS = /^[a-z0-9_]+$/;

const ALLOWED_TABLE_CHARS = /^[A-Za-z_]+[A-Za-z0-9_-]*$/;

// Dots are allowed in column names since there is no need for
// a schema/table delimiter when mapped to SQLite names.
const ALLOWED_COLUMN_CHARS = /^[A-Za-z_]+[.A-Za-z0-9_-]*$/;

export function validate(
  lc: LogContext,
  table: PublishedTableSpec,
  indexes: PublishedIndexSpec[],
) {
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
  if (table.replicaIdentity === Nothing) {
    throw new UnsupportedTableSchemaError(
      `Table "${table.name}" with REPLICA IDENTITY NOTHING cannot be replicated`,
    );
  }
  if (
    table.replicaIdentity === Index &&
    !indexes.some(
      idx =>
        idx.schema === table.schema &&
        idx.tableName === table.name &&
        idx.isReplicaIdentity,
    )
  ) {
    throw new UnsupportedTableSchemaError(
      `Table "${table.name}" is missing its REPLICA IDENTITY INDEX`,
    );
  }
  if (!ALLOWED_TABLE_CHARS.test(table.name)) {
    throw new UnsupportedTableSchemaError(
      `Table "${table.name}" has invalid characters.`,
    );
  }
  for (const [col, spec] of Object.entries(mapPostgresToLite(table).columns)) {
    if (!ALLOWED_COLUMN_CHARS.test(col)) {
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
