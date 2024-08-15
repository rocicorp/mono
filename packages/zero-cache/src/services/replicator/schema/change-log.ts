import type {Database} from 'better-sqlite3';
import {stringify} from 'zero-cache/src/types/bigint-json.js';
import type {LexiVersion} from 'zero-cache/src/types/lexi-version.js';
import {normalizedKeyOrder, type RowKey} from 'zero-cache/src/types/row-key.js';

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
 * The postgres TRUNCATE operation is represented as a log entry with a `null` row
 * key, which means that it is sorted before any row operations for that table.
 */

export const SET_OP = 's';
export const DEL_OP = 'd';
export const TRUNCATE_OP = 't';

const CREATE_CHANGELOG_SCHEMA =
  // stateVersion : a.k.a. row version
  // table        : The table associated with the change
  // rowKey       : JSON row key for a row change, or NULL for a table TRUNCATE.
  //                Note that SQLite will sort rows such that TRUNCATE operations appear
  //                before row operations (as desired), as NULL appears before non-NULL values.
  // op           : 't' for table truncation, 's' for set (insert/update), and 'd' for delete
  `
  CREATE TABLE "_zero.ChangeLog" (
    "stateVersion" TEXT NOT NULL,
    "table"        TEXT NOT NULL,
    "rowKey"       TEXT,
    "op"           TEXT NOT NULL,
    PRIMARY KEY("stateVersion", "table", "rowKey"),
    UNIQUE("table", "rowKey")
  )
  `;

export function initChangeLog(db: Database) {
  db.exec(CREATE_CHANGELOG_SCHEMA);
}

export function logSetOp(
  db: Database,
  version: LexiVersion,
  table: string,
  row: RowKey,
) {
  logRowOp(db, version, table, row, SET_OP);
}

export function logDeleteOp(
  db: Database,
  version: LexiVersion,
  table: string,
  row: RowKey,
) {
  logRowOp(db, version, table, row, DEL_OP);
}

function logRowOp(
  db: Database,
  version: LexiVersion,
  table: string,
  row: RowKey,
  op: string,
) {
  const rowKey = stringify(normalizedKeyOrder(row));
  db.prepare(
    `
    INSERT INTO "_zero.ChangeLog" (stateVersion, "table", rowKey, op)
      VALUES (@version, @table, JSON(@rowKey), @op)
      ON CONFLICT ("table", rowKey) DO UPDATE
      SET stateVersion = @version, op = @op
    `,
  ).run({version, table, rowKey, op});
}

export function logTruncateOp(
  db: Database,
  version: LexiVersion,
  table: string,
) {
  db.prepare(
    `
    DELETE FROM "_zero.ChangeLog" WHERE "table" = ?
    `,
  ).run(table);

  db.prepare(
    `
    INSERT INTO "_zero.ChangeLog" (stateVersion, "table", op) 
      VALUES (@version, @table, @op)
    `,
  ).run({version, table, op: TRUNCATE_OP});
}
