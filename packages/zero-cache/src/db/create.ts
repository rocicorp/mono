import type {LogContext} from '@rocicorp/logger';
import {SqliteError} from '@rocicorp/zero-sqlite3';
import type {Database} from '../../../zqlite/src/db.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../services/replicator/schema/constants.ts';
import {assertValidLiteColumnSpec} from '../types/lite.ts';
import {id, idList} from '../types/sql.ts';
import {liteIndexPredicateSQL} from './index-predicate.ts';
import type {ColumnSpec, LiteIndexSpec, LiteTableSpec} from './specs.ts';

/**
 * `spec` always comes from `mapPostgresToLiteColumn` so we only need to support
 * the output of that function.
 */
export function liteColumnDef(spec: ColumnSpec) {
  assertValidLiteColumnSpec(spec);

  let def = id(spec.dataType);

  if (spec.characterMaximumLength) {
    def += `(${spec.characterMaximumLength})`;
  }
  if (spec.notNull) {
    def += ' NOT NULL';
  }
  if (spec.dflt) {
    def += ` DEFAULT ${spec.dflt}`;
  }
  return def;
}

/**
 * Constructs a `CREATE TABLE` statement for a {@link TableSpec}.
 */
export function createLiteTableStatement(spec: LiteTableSpec): string {
  const defs = Object.entries(spec.columns)
    .sort(([_a, {pos: a}], [_b, {pos: b}]) => a - b)
    .map(([name, columnSpec]) => `${id(name)} ${liteColumnDef(columnSpec)}`);
  if (spec.primaryKey) {
    defs.push(`PRIMARY KEY (${idList(spec.primaryKey)})`);
  }

  const createStmt = `CREATE TABLE ${id(spec.name)} (`;
  return [createStmt, defs.join(',\n'), ');'].join('\n');
}

export function createLiteIndexStatement(index: LiteIndexSpec): string {
  const columns = Object.entries(index.columns)
    .map(([name, dir]) => `${id(name)} ${dir}`)
    .join(',');
  // Partial indexes are never unique on the replica. Their uniqueness is
  // not needed (a partial index cannot serve as a row key), and a UNIQUE
  // partial index would turn any divergence between PostgreSQL's and
  // SQLite's evaluation of the predicate into a replication failure.
  const unique = index.unique && !index.predicate ? 'UNIQUE' : '';
  const predicate = index.predicate
    ? ` WHERE ${liteIndexPredicateSQL(index.predicate)}`
    : '';
  return `CREATE ${unique} INDEX ${id(index.name)} ON ${id(
    index.tableName,
  )} (${columns})${predicate};`;
}

/**
 * Creates an index on the replica.
 *
 * Rows that are distinct upstream can have the same key on the replica, which
 * stores some types with less precision: a `numeric` is a double, and a `time`
 * is in milliseconds. A unique index over them then fails to build. Of such
 * rows only the one changed last is kept (of those changed in the same
 * transaction, the one added last), which is what replicating them one after
 * the other leaves, as each replaces the last.
 */
export function createLiteIndex(
  lc: LogContext,
  db: Database,
  index: LiteIndexSpec,
): void {
  const stmt = createLiteIndexStatement(index);
  try {
    db.exec(stmt);
    return;
  } catch (e) {
    if (!(e instanceof SqliteError && e.code === 'SQLITE_CONSTRAINT_UNIQUE')) {
      throw e;
    }
    const table = id(index.tableName);
    const tableCols = new Set(
      db
        .prepare(`SELECT lower(name) AS name FROM pragma_table_info(?)`)
        .all<{name: string}>(index.tableName)
        .map(({name}) => name),
    );
    // A column of the table with one of the names of the rowid hides it.
    const rowid = ROWID_NAMES.find(name => !tableCols.has(name));
    if (!rowid) {
      throw e;
    }
    const cols = Object.keys(index.columns).map(col => id(col));
    const latestFirst = [
      ...(tableCols.has(ZERO_VERSION_COLUMN_NAME)
        ? [`${id(ZERO_VERSION_COLUMN_NAME)} DESC`]
        : []),
      `${rowid} DESC`,
    ];
    // Rows with a NULL are distinct in a unique index.
    const {changes} = db
      .prepare(/*sql*/ `
        DELETE FROM ${table} WHERE ${rowid} IN (
          SELECT rid FROM (
            SELECT ${rowid} AS rid, ROW_NUMBER() OVER (
              PARTITION BY ${cols.join(',')} ORDER BY ${latestFirst.join(',')}
            ) AS n
            FROM ${table}
            WHERE ${cols.map(col => `${col} IS NOT NULL`).join(' AND ')}
          ) WHERE n > 1
        )`)
      .run();
    lc.warn?.(
      `Removed ${changes} row(s) of ${index.tableName} that have the same ` +
        `values for (${Object.keys(index.columns).join(', ')}) on the ` +
        `replica as a later row, to build the unique index ${index.name}`,
    );
  }
  db.exec(stmt);
}

const ROWID_NAMES = ['rowid', '_rowid_', 'oid'];
