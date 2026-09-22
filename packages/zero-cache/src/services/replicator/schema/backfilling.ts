/**
 * The replica's backfill cookie, keyed by upstream identity.
 *
 * `_zero.column_metadata.backfill` already records which columns are being
 * backfilled, and it stays authoritative for `isBackfilling`, which is what the
 * replica's own read paths use. What it cannot do is name the table an upstream
 * would recognize: it is keyed by *lite* table name, and `liteTableName()`
 * collapses the `public` schema and has no inverse anywhere in the tree. A lite
 * name containing a dot is ambiguous — `{public, "a.b"}` and `{a, b}` produce
 * the same string — and a table with backfilling columns and no
 * `_zero.tableMetadata` row (legal, since its metadata is optional) cannot be
 * resolved at all.
 *
 * That is the whole of what blocks a change log from being initialized out of a
 * restored replica, so this table carries the identity the reconstruction needs:
 * a straight row copy into the change log's own cookie tables, with nothing to
 * reconstruct and nothing to guess. The replica's copy is the *seed* source; the
 * change log's copy (`replicator/change-log-cookies.ts`) is the *working* set,
 * folded forward from the seed as the log's head advances.
 *
 * The cost is a second copy of `backfill` inside the replica, taken
 * deliberately — though not to save writes: `_zero.column_metadata` is written
 * only by initial sync, the DDL handlers, and backfill completion, so two more
 * columns on it would be free. It is a matter of shape. That table is keyed by
 * lite table name, so `schema`/`table` would be non-key duplicates of its own
 * key, to be held in sync with it across every rename with no answer to which
 * encoding is authoritative. This one is keyed exactly as `cdc.backfilling` and
 * `_zero.changeLogBackfilling` are, which is what makes the change log's
 * initialization comparison a row-set diff rather than a translation, and what
 * lets all three stores interpret the same fold. It also holds in-flight backfills and nothing else,
 * where the same query against `column_metadata` is an unindexed scan over a row
 * per column of every table.
 *
 * This table is write-only from the replica's perspective. Nothing in the
 * replicator reads it; it exists to be read by the change-streamer.
 */

import type {LogContext} from '@rocicorp/logger';
import {unreachable} from '../../../../../shared/src/asserts.ts';
import {BigIntJSON} from '../../../../../shared/src/bigint-json.ts';
import type {Database, Statement} from '../../../../../zqlite/src/db.ts';
import {getOrCreateCounter} from '../../../observability/metrics.ts';
import {liteTableName} from '../../../types/names.ts';
import type {
  BackfillID,
  Identifier,
  SchemaChange,
  TableMetadata,
} from '../../change-source/protocol/current/data.ts';
import type {BackfillRequest} from '../../change-source/protocol/current/upstream.ts';
import {
  backfillRequestsFrom,
  cookieOps,
  type CookieOp,
  type CookieSet,
} from '../change-log-cookies.ts';

export const BACKFILLING_TABLE = '_zero.backfilling';

// `backfill` holds the JSON that `cdc.backfilling` holds as JSONB. SQLite has
// no JSONB, and nothing here queries into the document: it is stored to be
// handed back to the change source verbatim.
export const CREATE_BACKFILLING_TABLE = /*sql*/ `
  CREATE TABLE "${BACKFILLING_TABLE}" (
    "schema"   TEXT NOT NULL,
    "table"    TEXT NOT NULL,
    "column"   TEXT NOT NULL,
    "backfill" TEXT NOT NULL,
    PRIMARY KEY ("schema", "table", "column")
  );
`;

/**
 * The replica's interpreter of the cookie fold in
 * `replicator/change-log-cookies.ts`.
 *
 * The fold is shared rather than re-implemented for the same reason the
 * Postgres and SQLite change logs share it: the three stores must agree on every
 * transition forever, and the failure mode when they drift is a backfill that is
 * silently never re-requested, on a column that is then silently never
 * populated. Sharing it makes the replica-versus-Postgres comparison a
 * regression test on the interpreters rather than on three implementations of
 * the same switch.
 *
 * Only the backfill half of the fold is interpreted here. The replica's metadata
 * cookie is `_zero.tableMetadata.upstreamMetadata`, which `TableMetadataTracker`
 * already maintains at the same sites, so an `upsert-metadata` op is inert.
 *
 * Statements are lazily prepared, matching `TableMetadataTracker`: a replica
 * that never sees a schema change never prepares any of them.
 */
export class BackfillingTracker {
  readonly #db: Database;

  #upsert: Statement | undefined;
  #renameTable: Statement | undefined;
  #dropTable: Statement | undefined;
  #renameColumn: Statement | undefined;
  #dropColumn: Statement | undefined;

  constructor(db: Database) {
    this.#db = db;
  }

  /**
   * Applies the change's backfill-cookie ops, returning the ops that were
   * applied. Changes that carry no backfill state — index changes, metadata-only
   * updates, and the `create-table` / `add-column` variants from a change source
   * that does not support backfill — apply nothing.
   */
  apply(change: SchemaChange): CookieOp[] {
    const ops = cookieOps(change);
    for (const op of ops) {
      this.#run(op);
    }
    return ops;
  }

  #run(op: CookieOp): void {
    switch (op.op) {
      case 'upsert-metadata':
        // Maintained by TableMetadataTracker in "_zero.tableMetadata".
        break;

      case 'upsert-backfill':
        (this.#upsert ??= this.#db.prepare(/*sql*/ `
          INSERT INTO "${BACKFILLING_TABLE}"
            ("schema", "table", "column", "backfill") VALUES (?, ?, ?, ?)
            ON CONFLICT ("schema", "table", "column")
            DO UPDATE SET "backfill" = excluded."backfill"
        `)).run(
          op.table.schema,
          op.table.name,
          op.column,
          BigIntJSON.stringify(op.backfill),
        );
        break;

      case 'rename-table':
        (this.#renameTable ??= this.#db.prepare(/*sql*/ `
          UPDATE "${BACKFILLING_TABLE}"
            SET "schema" = ?, "table" = ? WHERE "schema" = ? AND "table" = ?
        `)).run(op.new.schema, op.new.name, op.old.schema, op.old.name);
        break;

      case 'drop-table':
        (this.#dropTable ??= this.#db.prepare(/*sql*/ `
          DELETE FROM "${BACKFILLING_TABLE}"
            WHERE "schema" = ? AND "table" = ?
        `)).run(op.table.schema, op.table.name);
        break;

      case 'rename-column':
        (this.#renameColumn ??= this.#db.prepare(/*sql*/ `
          UPDATE "${BACKFILLING_TABLE}" SET "column" = ?
            WHERE "schema" = ? AND "table" = ? AND "column" = ?
        `)).run(op.new, op.table.schema, op.table.name, op.old);
        break;

      case 'drop-column':
        this.#deleteColumn(op.table, op.column);
        break;

      case 'complete-backfill':
        // A per-column delete rather than an `IN` list, which cannot be a
        // single prepared statement across arities. The columns of one
        // completed backfill are few, and this runs only on schema changes.
        for (const column of op.columns) {
          this.#deleteColumn(op.table, column);
        }
        break;

      default:
        unreachable(op);
    }
  }

  #deleteColumn(table: Identifier, column: string): void {
    (this.#dropColumn ??= this.#db.prepare(/*sql*/ `
      DELETE FROM "${BACKFILLING_TABLE}"
        WHERE "schema" = ? AND "table" = ? AND "column" = ?
    `)).run(table.schema, table.name, column);
  }
}

/**
 * Returns the {@link BackfillRequest}s from the current replica cookies. The
 * result has the same shape as the Postgres initialization result.
 *
 * The caller must pair this snapshot with the replica state version. When the
 * replica trails the change log, the comparison applies the missing schema
 * changes before it compares the results.
 */
export function readBackfillRequests(db: Database): BackfillRequest[] {
  return backfillRequestsFrom(readReplicaCookies(db));
}

/**
 * The replica's whole cookie set, in the shape the change log holds it — the
 * seed a change log would be initialized from, and the replica-derived half of
 * the initialization comparison.
 *
 * The metadata half is filtered on `upstreamMetadata IS NOT NULL` because
 * `_zero.tableMetadata` carries `minRowVersion` as well, so a row exists for
 * every table whose rows were ever force-re-downloaded, whether or not upstream
 * ever sent metadata for it. Neither cookie store can represent that row:
 * `cdc.tableMetadata."metadata"` and `_zero.changeLogTableMetadata."metadata"`
 * are both `NOT NULL`.
 *
 * Only meaningful paired with `_zero.replicationState.stateVersion`, read in
 * the same snapshot (invariant 15).
 */
export function readReplicaCookies(db: Database): CookieSet {
  const tableMetadata = db
    .prepare(/*sql*/ `
      SELECT "schema", "table", "upstreamMetadata" AS "metadata"
        FROM "_zero.tableMetadata"
        WHERE "upstreamMetadata" IS NOT NULL
        ORDER BY "schema", "table"
    `)
    .all<{schema: string; table: string; metadata: string}>()
    .map(({schema, table, metadata}) => ({
      schema,
      table,
      metadata: BigIntJSON.parse(metadata) as TableMetadata,
    }));

  const backfilling = db
    .prepare(/*sql*/ `
      SELECT "schema", "table", "column", "backfill"
        FROM "${BACKFILLING_TABLE}"
        ORDER BY "schema", "table", "column"
    `)
    .all<{schema: string; table: string; column: string; backfill: string}>()
    .map(({schema, table, column, backfill}) => ({
      schema,
      table,
      column,
      backfill: BigIntJSON.parse(backfill) as BackfillID,
    }));

  return {tableMetadata, backfilling};
}

/**
 * Seeds {@link BACKFILLING_TABLE} from `_zero.column_metadata`, resolving each
 * lite table name back to its upstream identity. Used by the v17 migration.
 *
 * The resolution is exact for a table that has a `_zero.tableMetadata` row, and
 * exact for a lite name with no dot in it — `liteTableName()` only omits the
 * schema for `public`. What is left over is an in-flight backfill, on a
 * non-`public` schema, on a table with no metadata row, at the instant of
 * upgrade. Those get the best-effort first-dot split and are counted, because a
 * wrong identity produces a `BackfillRequest` the change source will reject
 * rather than a silently wrong one, and because the initialization comparison
 * surfaces it before anything depends on it. The expected count is zero.
 *
 * A full replace rather than an insert-if-absent: `migrateData` re-runs after a
 * rollback and roll-forward, during which a v16 zero-cache advanced the replica
 * without knowing this table existed. Rebuilding it from `column_metadata` —
 * which that zero-cache *did* maintain — is what makes the re-run both
 * idempotent and correct.
 */
export function populateBackfillingFromColumnMetadata(
  lc: LogContext,
  db: Database,
): void {
  db.prepare(/*sql*/ `DELETE FROM "${BACKFILLING_TABLE}"`).run();

  const backfilling = db
    .prepare(/*sql*/ `
      SELECT table_name, column_name, backfill FROM "_zero.column_metadata"
        WHERE backfill IS NOT NULL
        ORDER BY table_name, column_name
    `)
    .all<{table_name: string; column_name: string; backfill: string}>();

  const unresolvable = getOrCreateCounter(
    'replica',
    'backfilling_unresolvable_rows',
    'In-flight backfills whose upstream table identity could not be recovered ' +
      'from the replica when "' +
      BACKFILLING_TABLE +
      '" was created. A best-effort identity is stored; the expected count is 0.',
  );
  if (backfilling.length === 0) {
    unresolvable.add(0); // the common case: publish the series anyway
    return;
  }

  const byLiteName = new Map<string, Identifier>(
    db
      .prepare(/*sql*/ `SELECT "schema", "table" FROM "_zero.tableMetadata"`)
      .all<{schema: string; table: string}>()
      .map(({schema, table}) => [
        liteTableName({schema, name: table}),
        {schema, name: table},
      ]),
  );

  const insert = db.prepare(/*sql*/ `
    INSERT INTO "${BACKFILLING_TABLE}"
      ("schema", "table", "column", "backfill") VALUES (?, ?, ?, ?)
  `);
  let unresolved = 0;
  for (const {table_name: liteName, column_name, backfill} of backfilling) {
    const known = byLiteName.get(liteName);
    const dot = known ? -1 : liteName.indexOf('.');
    const table = known ?? {
      schema: dot < 0 ? 'public' : liteName.slice(0, dot),
      name: dot < 0 ? liteName : liteName.slice(dot + 1),
    };
    if (!known && dot >= 0) {
      unresolved++;
      lc.warn?.(
        `no metadata for backfilling table "${liteName}"; ` +
          `assuming {schema: "${table.schema}", name: "${table.name}"}`,
      );
    }
    insert.run(table.schema, table.name, column_name, backfill);
  }
  unresolvable.add(unresolved);
  lc.info?.(
    `seeded "${BACKFILLING_TABLE}" with ${backfilling.length} in-flight ` +
      `backfill(s), ${unresolved} of which could not be resolved`,
  );
}
