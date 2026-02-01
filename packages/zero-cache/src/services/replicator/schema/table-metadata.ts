import type {Database, Statement} from '../../../../../zqlite/src/db.ts';
import type {
  Identifier,
  TableMetadata,
} from '../../change-source/protocol/current.ts';

/**
 * Replica-level analog of tableMetadata in change-streamer/schema.
 * Per the requirement of the backfill protocol, backfill metadata
 * must be tracked outside of the change source (otherwise the change
 * source would have to be able to compute the state of the metadata at
 * arbitrary points in the past).
 *
 * This tracking is done:
 * 1. at the Change DB level, by the change-streamer
 * 2. at the replica level, in order to support the eventual configuration
 *    of ephemeral Change DBs (on SQLite) that are initialized from data
 *    in the replica.
 */
export const CREATE_TABLE_METADATA_TABLE = /*sql*/ `
  CREATE TABLE "_zero.tableMetadata" (
    "schema"   TEXT NOT NULL,
    "table"    TEXT NOT NULL,
    "metadata" TEXT NOT NULL,
    PRIMARY KEY ("schema", "table")
  );
`;

export class TableMetadataTracker {
  readonly #set: Statement;
  readonly #rename: Statement;
  readonly #drop: Statement;

  constructor(db: Database) {
    this.#set = db.prepare(/*sql*/ `
      INSERT OR REPLACE INTO "_zero.tableMetadata"
        ("schema", "table", "metadata") VALUES (?, ?, ?);
    `);
    this.#rename = db.prepare(/*sql*/ `
      UPDATE "_zero.tableMetadata" SET "schema" = ?, "table" = ?
        WHERE "schema" = ? AND "table" = ?
    `);
    this.#drop = db.prepare(/*sql*/ `
      DELETE FROM "_zero.tableMetadata" WHERE "schema" = ? AND "table" = ?
    `);
  }

  set({schema, name}: Identifier, metadata: TableMetadata) {
    this.#set.run(schema, name, JSON.stringify(metadata));
  }

  rename(oldTable: Identifier, newTable: Identifier) {
    this.#rename.run(
      newTable.schema,
      newTable.name,
      oldTable.schema,
      oldTable.name,
    );
  }

  drop({schema, name}: Identifier) {
    this.#drop.run(schema, name);
  }
}
