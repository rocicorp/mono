import type {LogContext} from '@rocicorp/logger';
import {
  mapPostgresToLite,
  warnIfDataTypeSupported,
} from '../../../../db/pg-to-lite.ts';
import {
  Default,
  Full,
  Index,
  Nothing,
} from '../../../../db/postgres-replica-identity-enum.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../../../replicator/schema/constants.ts';
import type {PublishedTableWithReplicaIdentity} from './published.ts';

export const ALLOWED_APP_ID_CHARACTERS = /^[a-z0-9_]+$/;

const ALLOWED_TABLE_CHARS = /^[A-Za-z_]+[A-Za-z0-9_-]*$/;

// Dots are allowed in column names since there is no need for
// a schema/table delimiter when mapped to SQLite names.
const ALLOWED_COLUMN_CHARS = /^[A-Za-z_]+[.A-Za-z0-9_-]*$/;

export function validate(
  lc: LogContext,
  table: PublishedTableWithReplicaIdentity,
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
    table.replicaIdentityColumns.length === 0
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
  warnIfRowFilterOutsideReplicaIdentity(lc, table);
}

/**
 * Postgres accepts a publication row filter on any column, but rejects
 * UPDATE and DELETE on the table (upstream, in the application's own
 * transactions) when a filtered column is not part of the replica identity.
 * Tables that only receive INSERTs keep working, so this is a warning.
 */
function warnIfRowFilterOutsideReplicaIdentity(
  lc: LogContext,
  table: PublishedTableWithReplicaIdentity,
) {
  if (table.replicaIdentity === Full) {
    return;
  }
  const identity = new Set(table.replicaIdentityColumns);
  for (const [publication, {rowFilter}] of Object.entries(table.publications)) {
    if (rowFilter === null) {
      continue;
    }
    const uncovered = [...rowFilterColumns(rowFilter, table)].filter(
      col => !identity.has(col),
    );
    if (uncovered.length) {
      lc.warn?.(
        `Row filter of publication "${publication}" on table "${table.name}" ` +
          `references ${uncovered.map(c => `"${c}"`).join(', ')}, which ` +
          `${uncovered.length === 1 ? 'is' : 'are'} not ` +
          `part of the table's REPLICA IDENTITY. Postgres will reject UPDATE ` +
          `and DELETE on "${table.name}". Add the column(s) to a unique index ` +
          `and set it with 'ALTER TABLE ... REPLICA IDENTITY USING INDEX', ` +
          `or use 'REPLICA IDENTITY FULL'.`,
      );
    }
  }
}

const ROW_FILTER_TOKEN = /'(?:[^']|'')*'|"(?:[^"]|"")*"|[A-Za-z_]\w*|::|\S/g;
const LOWER_CASE_NAME = /^[a-z_][a-z0-9_]*$/;

/**
 * Returns the table columns referenced by a row filter as deparsed by
 * `pg_get_expr()`, which prints keywords in upper case, double-quotes any
 * name that is not a plain lower-case identifier, and renders casts as
 * `::type`. Any other name is a column, unless it is called as a function,
 * qualifies another name, names a type, or names a collation.
 */
function rowFilterColumns(
  rowFilter: string,
  table: PublishedTableWithReplicaIdentity,
): Set<string> {
  const tokens = rowFilter.match(ROW_FILTER_TOKEN) ?? [];
  const columns = new Set<string>();
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (token === '::') {
      while (isName(tokens[i + 1]) || tokens[i + 1] === '.') {
        i++;
      }
      continue;
    }
    if (
      !isName(token) ||
      tokens[i + 1] === '(' ||
      tokens[i + 1] === '.' ||
      tokens[i - 1] === 'COLLATE'
    ) {
      continue;
    }
    const name = token.startsWith('"')
      ? token.slice(1, -1).replaceAll('""', '"')
      : token;
    if (Object.hasOwn(table.columns, name)) {
      columns.add(name);
    }
  }
  return columns;
}

function isName(token: string | undefined): token is string {
  return (
    token !== undefined &&
    (token.startsWith('"') ||
      (LOWER_CASE_NAME.test(token) && token !== 'true' && token !== 'false'))
  );
}

export class UnsupportedTableSchemaError extends Error {
  readonly name = 'UnsupportedTableSchemaError';

  constructor(msg: string) {
    super(msg);
  }
}
