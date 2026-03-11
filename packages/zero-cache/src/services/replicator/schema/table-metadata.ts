import type {Database, Statement} from '../../../../../zqlite/src/db.ts';
import {liteTableName} from '../../../types/names.ts';
import type {
  Identifier,
  TableMetadata,
} from '../../change-source/protocol/current.ts';

/**
 * Table-level controls for handling replicated data.
 *
 * ### Columns
 *
 * `minRowVersion`: the minimum `_0_version` value to apply to
 *   all rows in the table. This overrides any per-row
 *   `_0_version` value that is smaller (i.e. earlier).
 *   The `minRowVersion` column is used to force a re-download
 *   of all rows after a table-wide schema change (by giving
 *   each row a version that's newer than what's in any CVR).
 *   The naive, brute-force method of updating all of the rows
 *   requires re-writing the entire table into the WAL as one
 *   SQLite operation, which is too costly from both latency
 *   and storage space.
 *
 * `upstreamMetadata`: the replica-level analog of tableMetadata in
 *   change-streamer/schema. Per the requirement of the backfill
 *   protocol, backfill metadata must be tracked outside of the
 *   change source (otherwise the change source would have to be
 *   able to compute the state of the metadata at arbitrary points
 *   in the past).
 *
 * `metadata`: the previous name of the `upstreamMetadata` column,
 *   kept for backwards compatibility.
 *
 * This tracking is done:
 * 1. at the Change DB level, by the change-streamer
 * 2. at the replica level, in order to support the eventual configuration
 *    of ephemeral Change DBs (on SQLite) that are initialized from data
 *    in the replica.
 */
export const CREATE_TABLE_METADATA_TABLE = /*sql*/ `
  CREATE TABLE "_zero.tableMetadata" (
    "schema"           TEXT NOT NULL,
    "table"            TEXT NOT NULL,
    "minRowVersion"    TEXT NOT NULL DEFAULT "00",
    "upstreamMetadata" TEXT,
    "metadata"         TEXT,  -- deprecated
    PRIMARY KEY ("schema", "table")
  );
`;

export class TableMetadataTracker {
  readonly #db: Database;

  // All statements are lazily created.
  #setUpstreamMetadata: Statement | undefined;
  #setMinRowVersion: Statement | undefined;
  #getMinRowVersions: Statement | undefined;
  #rename: Statement | undefined;
  #drop: Statement | undefined;

  constructor(db: Database) {
    this.#db = db;
  }

  setUpstreamMetadata({schema, name}: Identifier, metadata: TableMetadata) {
    (this.#setUpstreamMetadata ??= this.#db.prepare(/*sql*/ `
      INSERT INTO "_zero.tableMetadata" ("schema", "table", "upstreamMetadata") 
        VALUES (@schema, @name, @metadata)
        ON CONFLICT ("schema", "table")
        DO UPDATE SET "upstreamMetadata" = @metadata
    `)).run({
      schema,
      name,
      metadata: JSON.stringify(metadata),
    });
  }

  setMinRowVersion({schema, name}: Identifier, version: string) {
    (this.#setMinRowVersion ??= this.#db.prepare(/*sql*/ `
      INSERT INTO "_zero.tableMetadata" ("schema", "table", "minRowVersion") 
        VALUES (@schema, @name, @version)
        ON CONFLICT ("schema", "table")
        DO UPDATE SET "minRowVersion" = @version;
    `)).run({schema, name, version});
  }

  getMinRowVersions(): Map<string, string> {
    const results = (this.#getMinRowVersions ??= this.#db.prepare(/*sql*/ `
      SELECT "schema", "table" as "name", "minRowVersion" FROM "_zero.tableMetadata"
    `)).all<{schema: string; name: string; minRowVersion: string}>();
    return new Map(
      results.map(({schema, name, minRowVersion}) => [
        liteTableName({schema, name}),
        minRowVersion,
      ]),
    );
  }

  rename(oldTable: Identifier, newTable: Identifier) {
    (this.#rename ??= this.#db.prepare(/*sql*/ `
      UPDATE "_zero.tableMetadata" SET "schema" = ?, "table" = ?
        WHERE "schema" = ? AND "table" = ?
    `)).run(newTable.schema, newTable.name, oldTable.schema, oldTable.name);
  }

  drop({schema, name}: Identifier) {
    (this.#drop ??= this.#db.prepare(/*sql*/ `
      DELETE FROM "_zero.tableMetadata" WHERE "schema" = ? AND "table" = ?
    `)).run(schema, name);
  }
}
