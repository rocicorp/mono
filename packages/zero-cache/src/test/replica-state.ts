import type {Database} from '../../../zqlite/src/db.ts';
import {listIndexes, listTables} from '../db/lite-tables.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../services/replicator/schema/replication-state.ts';
import {id} from '../types/sql.ts';

type CanonicalValue =
  | null
  | boolean
  | number
  | string
  | CanonicalValue[]
  | {[key: string]: CanonicalValue};

export type CanonicalReplicaState = ReturnType<typeof canonicalReplicaState>;

/**
 * Returns the user-visible and physical state of a replica in a form suitable
 * for comparing an incrementally updated replica with a freshly built one.
 * Replication watermarks and row versions are intentionally excluded.
 */
export function canonicalReplicaState(db: Database) {
  const physicalTables = listTables(db, false, false)
    .map(({name, columns}) => ({
      name,
      columns: Object.entries(columns)
        .filter(([column]) => column !== ZERO_VERSION_COLUMN_NAME)
        .map(([column, spec], pos) => ({column, ...spec, pos: pos + 1})),
    }))
    .sort(byName);

  const logicalTables = listTables(db, true, false)
    .map(({name, columns}) => ({
      name,
      columns: Object.entries(columns)
        .filter(([column]) => column !== ZERO_VERSION_COLUMN_NAME)
        .map(([column, spec], pos) => ({column, ...spec, pos: pos + 1})),
    }))
    .sort(byName);

  const indexes = listIndexes(db)
    .map(index => ({
      ...index,
      columns: Object.entries(index.columns),
    }))
    .sort(byName);

  const columnMetadata = db
    .prepare(
      `SELECT table_name, column_name, upstream_type, is_not_null,
              is_enum, is_array, character_max_length, backfill
         FROM "_zero.column_metadata"
        ORDER BY table_name, column_name`,
    )
    .all();

  const rows = physicalTables.map(({name, columns}) => ({
    name,
    rows: db
      .prepare(
        `SELECT ${columns.map(({column}) => id(column)).join(', ')}
           FROM ${id(name)}`,
      )
      .safeIntegers(true)
      .all<Record<string, unknown>>()
      .map(canonicalizeRow)
      .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))),
  }));

  const integrityCheck = db
    .prepare('PRAGMA integrity_check')
    .all<Record<string, unknown>>()
    .map(canonicalizeRow);

  return {
    physicalTables,
    logicalTables,
    indexes,
    columnMetadata,
    rows,
    integrityCheck,
  };
}

function byName<T extends {name: string}>(a: T, b: T) {
  return a.name.localeCompare(b.name);
}

function canonicalizeRow(row: Record<string, unknown>) {
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [key, canonicalizeValue(value)]),
  );
}

function canonicalizeValue(value: unknown): CanonicalValue {
  if (
    value === null ||
    typeof value === 'boolean' ||
    typeof value === 'number' ||
    typeof value === 'string'
  ) {
    return value;
  }
  if (typeof value === 'bigint') {
    return {bigint: value.toString()};
  }
  if (value instanceof Uint8Array) {
    return {bytes: Buffer.from(value).toString('hex')};
  }
  if (Array.isArray(value)) {
    return value.map(canonicalizeValue);
  }
  if (typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, item]) => [
        key,
        canonicalizeValue(item),
      ]),
    );
  }
  throw new TypeError(`Unsupported SQLite value type: ${typeof value}`);
}
