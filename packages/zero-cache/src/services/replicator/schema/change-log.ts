import {
  jsonObjectSchema,
  parse,
  stringify,
} from '../../../../../shared/src/bigint-json.ts';
import * as v from '../../../../../shared/src/valita.ts';
import type {Database, Statement} from '../../../../../zqlite/src/db.ts';
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

// Exported for testing (and migrations)
export const CREATE_CHANGELOG_SCHEMA =
  // stateVersion : a.k.a. row version
  // pos          : order in which to process the change (within the version)
  // table        : The table associated with the change
  // rowKey       : JSON row key for a row change. For table-wide changes RESET
  //                and TRUNCATE, there is no associated row; instead, `pos` is
  //                set to -1 and the rowKey is set to the stateVersion,
  //                guaranteeing when attempting to process the transaction,
  //                the pipeline is reset (and the change log traversal
  //                aborted).
  // op           : 's' for set (insert/update)
  //              : 'd' for delete
  //              : 'r' for table reset (schema change)
  //              : 't' for table truncation (which also resets the pipeline)
  // backfillingColumnVersions
  //              : A JSON mapping from column name to stateVersion tracked
  //                for replicated writes of columns that are being backfilled.
  //                This is used to prevent backfill data, which is at a
  //                fixed snapshot/version outside of the replication stream,
  //                from overwriting newer column values.
  //
  // Naming note: To maintain compatibility between a new replication-manager
  // and old view-syncers, the previous _zero.changeLog table is preserved
  // and its replacement given a new name "changeLog2".
  `
  CREATE TABLE "_zero.changeLog2" (
    "stateVersion"              TEXT NOT NULL,
    "pos"                       INT  NOT NULL,
    "table"                     TEXT NOT NULL,
    "rowKey"                    TEXT NOT NULL,
    "op"                        TEXT NOT NULL,
    "backfillingColumnVersions" TEXT DEFAULT '{}',
    PRIMARY KEY("stateVersion", "pos"),
    UNIQUE("table", "rowKey")
  );
  `;

/**
 * Contains the changeLog fields relevant for computing the diff between
 * two snapshots of a replica. The `pos` and `backfillingColumnVersions`
 * fields are excluded, though the query should be ordered by
 * `<stateVersion, pos>`.
 */
export const changeLogEntrySchema = v
  .object({
    stateVersion: v.string(),
    table: v.string(),
    rowKey: v.string(),
    op: v.literalUnion(SET_OP, DEL_OP, TRUNCATE_OP, RESET_OP),
  })
  .map(val => ({
    ...val,
    // Note: sets the rowKey to `null` for table-wide ops / resets
    rowKey:
      val.op === 't' || val.op === 'r'
        ? null
        : v.parse(parse(val.rowKey), jsonObjectSchema),
  }));

export type ChangeLogEntry = v.Infer<typeof changeLogEntrySchema>;

export class ChangeLog {
  readonly #logRowOpStmt: Statement;
  readonly #logRowOpWithBackfillStmt: Statement;
  readonly #logTableWideOpStmt;

  constructor(db: Database) {
    this.#logRowOpStmt = db.prepare(/*sql*/ `
      INSERT OR REPLACE INTO "_zero.changeLog2" 
        (stateVersion, pos, "table", rowKey, op)
        VALUES (@version, @pos, @table, JSON(@rowKey), @op)
    `);

    this.#logRowOpWithBackfillStmt = db.prepare(/*sql*/ `
      INSERT INTO "_zero.changeLog2" 
        (stateVersion, pos, "table", rowKey, op, backfillingColumnVersions)
        VALUES (@version, @pos, @table, JSON(@rowKey), @op, 
                JSON(@backfillingColumnVersions))
        ON CONFLICT ("table", rowKey) DO UPDATE 
                   SET stateVersion = excluded.stateVersion,
                                pos = excluded.pos,
                                 op = excluded.op,
          backfillingColumnVersions = json_patch(
          backfillingColumnVersions, excluded.backfillingColumnVersions)
    `);

    // Because table-wide ops result in aborting an incremental update
    // and rehydrating all queries at "head", they are assigned pos = -1
    // as an optimization to abort as early as possible to skip unnecessary
    // updates.
    //
    // However, changeLog entries that are destined to be "skipped" are
    // nonetheless kept for the purpose of tracking backfillingColumnVersions.
    this.#logTableWideOpStmt = db.prepare(/*sql*/ `
      INSERT OR REPLACE INTO "_zero.changeLog2" 
        (stateVersion, pos, "table", rowKey, op) 
        VALUES (@version, -1, @table, @version, @op)
    `);
  }

  /**
   *
   * @param backfilled The backfilling columns for which values were set. Note
   *   that an empty list and the `undefined` value mean different things;
   *   * An empty list indicates that a backfill is in progress but no
   *     backfilling values were set. In this case, existing
   *     backfillingColumnVersions are preserved.
   *   * `undefined` indicates that there are no columns being backfilled.
   *     In this case, any vestigial `backfillingColumnVersions` value
   *     is cleared.
   */
  logSetOp(
    version: LexiVersion,
    pos: number,
    table: string,
    row: LiteRowKey,
    backfilled: string[] | undefined,
  ): string {
    return this.#logRowOp(version, pos, table, row, SET_OP, backfilled);
  }

  logDeleteOp(
    version: LexiVersion,
    pos: number,
    table: string,
    row: LiteRowKey,
  ): string {
    // Note: For delete ops, it is always safe to clear the
    //       backfillingColumnVersions because the backfill algorithm
    //       understands that deletes apply to the whole row.
    return this.#logRowOp(version, pos, table, row, DEL_OP, undefined);
  }

  #logRowOp(
    version: LexiVersion,
    pos: number,
    table: string,
    row: LiteRowKey,
    op: string,
    backfilled: string[] | undefined,
  ): string {
    const rowKey = stringify(normalizedKeyOrder(row));
    if (backfilled === undefined) {
      this.#logRowOpStmt.run({version, pos, table, rowKey, op});
    } else {
      const versions: Record<string, string> = {};
      for (const col of backfilled) {
        versions[col] = version;
      }
      this.#logRowOpWithBackfillStmt.run({
        version,
        pos,
        table,
        rowKey,
        op,
        backfillingColumnVersions: JSON.stringify(versions),
      });
    }
    return rowKey;
  }

  logTruncateOp(version: LexiVersion, table: string) {
    this.#logTableWideOpStmt.run({version, table, op: TRUNCATE_OP});
  }

  logResetOp(version: LexiVersion, table: string) {
    this.#logTableWideOpStmt.run({version, table, op: RESET_OP});
  }
}
