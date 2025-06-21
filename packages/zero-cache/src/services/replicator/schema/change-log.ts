import * as v from '../../../../../shared/src/valita.ts';
import type {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {
  jsonObjectSchema,
  parse,
  stringify,
} from '../../../../../shared/src/bigint-json.ts';
import type {LexiVersion} from '../../../types/lexi-version.ts';
import type {LiteRowKey} from '../../../types/lite.ts';
import {normalizedKeyOrder} from '../../../types/row-key.ts';

/**
 * The Change Log tracks the last operation (set or delete) for each row in the
 * data base, ordered by state version; in other words, a cross-table
 * index of row changes ordered by version. This facilitates a minimal "diff"
 * of row changes needed to advance a pipeline from one state version to another.
 *
 * The Change Log stores identifiers only, i.e. it does not store contents.
 * A database snapshot at the previous version can be used to query a row's
 * old contents, if any, and the current snapshot can be used to query a row's
 * new contents. (In the common case, the new contents will have just been applied
 * and thus has a high likelihood of being in the SQLite cache.)
 *
 * There are two table-wide operations:
 * - `t` corresponds to the postgres `TRUNCATE` operation
 * - `r` represents any schema (i.e. column) change
 *
 * For both operations, the corresponding row changes are not explicitly included
 * in the change log. The consumer has the option of simulating them be reading
 * from pre- and post- snapshots, or resetting their state entirely with the current
 * snapshot.
 *
 * To achieve the desired ordering semantics when processing tables that have been
 * truncated, reset, and modified, the "rowKey" is set to `null` for resets and
 * the empty string `""` for truncates. This means that resets will be encountered
 * before truncates, which will be processed before any subsequent row changes.
 *
 * This ordering is chosen because resets are currently the more "destructive" op
 * and result in aborting the processing (and starting from scratch); doing this
 * earlier reduces wasted work.
 */

export const SET_OP = 's';
export const DEL_OP = 'd';
export const TRUNCATE_OP = 't';
export const RESET_OP = 'r';

const CREATE_CHANGELOG_SCHEMA =
  // stateVersion : a.k.a. row version
  // table        : The table associated with the change
  // rowKey       : JSON row key for a row change. For table-wide changes,
  //                this is set to '', which guarantees that they sort before
  //                any (subsequent) row-level changes. Note that because
  //                RESET and TRUNCATE use the same rowKey, only the last
  //                one will persist. This is fine because they are both
  //                handled in the same way, i.e. by resetting the pipelines,
  //                as they cannot be processed via change log entries.
  // op           : 't' for table truncation
  //              : 'r' for table reset (schema change)
  //                's' for set (insert/update)
  //                'd' for delete
  `
  CREATE TABLE "_zero.changeLog" (
    "stateVersion" TEXT NOT NULL,
    "table"        TEXT NOT NULL,
    "rowKey"       TEXT,
    "op"           TEXT NOT NULL,
    PRIMARY KEY("stateVersion", "table", "rowKey"),
    UNIQUE("table", "rowKey")
  )
  `;

export const changeLogEntrySchema = v
  .object({
    stateVersion: v.string(),
    table: v.string(),
    rowKey: v.string().nullable(),
    op: v.literalUnion(SET_OP, DEL_OP, TRUNCATE_OP, RESET_OP),
  })
  .map(val => ({
    ...val,
    // Note: the empty string "" (for table-wide ops) will result in `null`
    rowKey: val.rowKey ? v.parse(parse(val.rowKey), jsonObjectSchema) : null,
  }));

export type ChangeLogEntry = v.Infer<typeof changeLogEntrySchema>;

export function initChangeLog(db: Database) {
  db.exec(CREATE_CHANGELOG_SCHEMA);
}

export function logSetOp(
  db: StatementRunner,
  version: LexiVersion,
  table: string,
  row: LiteRowKey,
): string {
  return logRowOp(db, version, table, row, SET_OP);
}

export function logDeleteOp(
  db: StatementRunner,
  version: LexiVersion,
  table: string,
  row: LiteRowKey,
): string {
  return logRowOp(db, version, table, row, DEL_OP);
}

function logRowOp(
  db: StatementRunner,
  version: LexiVersion,
  table: string,
  row: LiteRowKey,
  op: string,
): string {
  const rowKey = stringify(normalizedKeyOrder(row));
  db.run(
    `
    INSERT OR REPLACE INTO "_zero.changeLog" 
      (stateVersion, "table", rowKey, op)
      VALUES (@version, @table, JSON(@rowKey), @op)
    `,
    {version, table, rowKey, op},
  );
  return rowKey;
}
export function logTruncateOp(
  db: StatementRunner,
  version: LexiVersion,
  table: string,
) {
  logTableWideOp(db, version, table, TRUNCATE_OP);
}

export function logResetOp(
  db: StatementRunner,
  version: LexiVersion,
  table: string,
) {
  logTableWideOp(db, version, table, RESET_OP);
}

function logTableWideOp(
  db: StatementRunner,
  version: LexiVersion,
  table: string,
  op: 't' | 'r',
) {
  // Delete any existing changes for the table (in this version) since the
  // table wide op invalidates them.
  db.run(
    `
    DELETE FROM "_zero.changeLog" WHERE stateVersion = ? AND "table" = ?
    `,
    version,
    table,
  );

  db.run(
    `
    INSERT OR REPLACE INTO "_zero.changeLog" (stateVersion, "table", rowKey, op) 
      VALUES (@version, @table, @rowKey, @op)
    `,
    // See file JSDoc for explanation of the rowKey w.r.t. ordering of table-wide ops.
    {version, table, rowKey: '', op},
  );
}
