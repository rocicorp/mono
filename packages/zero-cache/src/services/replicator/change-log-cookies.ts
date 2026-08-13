/**
 * The change log's *second* job.
 *
 * A change log is a buffer **and** a cookie jar. The buffer catches a
 * reconnecting subscriber up to the head; the cookie jar holds the backfill
 * progress state that the change-streamer hands the change-source on every
 * `startStream`, so that the change source never has to persist backfill state
 * upstream. See `change-log-db.ts` for the buffer.
 *
 * There are two cookies, mirroring the two `cdc` tables the Postgres change log
 * has always carried (`change-streamer/schema/tables.ts`):
 *
 * - **table metadata**, the most current {@link TableMetadata} of a table, which
 *   a {@link BackfillRequest} has to carry; and
 * - **backfilling columns**, the opaque {@link BackfillID} of every column whose
 *   backfill has not completed.
 *
 * Both are a *fold over the change stream*: the cookie set is only meaningful
 * paired with the watermark it was folded up to. {@link cookieOps} is that fold,
 * expressed transport-free so that the Postgres change log and the SQLite change
 * log cannot drift from each other — they must agree on all eight transitions
 * forever, and the failure mode if they disagree is a backfill that is silently
 * never re-requested. The two interpreters are {@link ChangeLogCookieWriter}
 * here and `Storer.#trackBackfillMetadata()` in
 * `change-streamer/storer.ts`.
 *
 * The tables live in the change-log database rather than in the replica: the
 * replicator is a subscriber *downstream* of the change-streamer's write loop,
 * so its state version trails the log's head, and pairing its cookies with the
 * log's resume watermark would lose every backfill that completed in between.
 * The replica's own copies (`_zero.tableMetadata`, `_zero.column_metadata`) are
 * the *seed* source; these are the *working* set.
 */

import {unreachable} from '../../../../shared/src/asserts.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import * as v from '../../../../shared/src/valita.ts';
import type {Database, Statement} from '../../../../zqlite/src/db.ts';
import type {
  BackfillID,
  Identifier,
  SchemaChange,
  TableMetadata,
} from '../change-source/protocol/current/data.ts';
import {
  backfillRequestSchema,
  type BackfillRequest,
} from '../change-source/protocol/current/upstream.ts';

export const CHANGE_LOG_TABLE_METADATA_TABLE = '_zero.changeLogTableMetadata';
export const CHANGE_LOG_BACKFILLING_TABLE = '_zero.changeLogBackfilling';

// The names follow the file's existing convention, beside
// "_zero.changeLogStream" and "_zero.changeLogMeta".
//
// `metadata` and `backfill` hold the JSON that the `cdc` tables hold as JSONB.
// SQLite has no JSONB, and nothing here queries into the documents: they are
// stored to be handed back to the change source verbatim.
export const CREATE_CHANGE_LOG_COOKIE_SCHEMA = /*sql*/ `
  CREATE TABLE "${CHANGE_LOG_TABLE_METADATA_TABLE}" (
    "schema"   TEXT NOT NULL,
    "table"    TEXT NOT NULL,
    "metadata" TEXT NOT NULL,
    PRIMARY KEY ("schema", "table")
  );

  CREATE TABLE "${CHANGE_LOG_BACKFILLING_TABLE}" (
    "schema"   TEXT NOT NULL,
    "table"    TEXT NOT NULL,
    "column"   TEXT NOT NULL,
    "backfill" TEXT NOT NULL,
    PRIMARY KEY ("schema", "table", "column")
  );
`;

export const DROP_CHANGE_LOG_COOKIE_TABLES = /*sql*/ `
  DROP TABLE IF EXISTS "${CHANGE_LOG_TABLE_METADATA_TABLE}";
  DROP TABLE IF EXISTS "${CHANGE_LOG_BACKFILLING_TABLE}";
`;

export type TableMetadataCookie = {
  readonly schema: string;
  readonly table: string;
  readonly metadata: TableMetadata;
};

export type BackfillCookie = {
  readonly schema: string;
  readonly table: string;
  readonly column: string;
  readonly backfill: BackfillID;
};

/**
 * The whole cookie set, as of the log's head. Both lists are ordered by their
 * primary key so that two stores' sets can be compared directly.
 */
export type CookieSet = {
  readonly tableMetadata: readonly TableMetadataCookie[];
  readonly backfilling: readonly BackfillCookie[];
};

export const EMPTY_COOKIE_SET: CookieSet = {
  tableMetadata: [],
  backfilling: [],
};

/**
 * A transport-free description of a cookie mutation. This is the whole of what
 * a {@link SchemaChange} means to the cookie jar; everything else about the
 * change belongs to the buffer.
 */
export type CookieOp =
  | {op: 'upsert-metadata'; table: Identifier; metadata: TableMetadata}
  | {
      op: 'upsert-backfill';
      table: Identifier;
      column: string;
      backfill: BackfillID;
    }
  | {op: 'rename-table'; old: Identifier; new: Identifier}
  | {op: 'drop-table'; table: Identifier}
  | {op: 'rename-column'; table: Identifier; old: string; new: string}
  | {op: 'drop-column'; table: Identifier; column: string}
  | {op: 'complete-backfill'; table: Identifier; columns: readonly string[]};

export type CookieOpTag = CookieOp['op'];

/**
 * The fold, and the only place either store decides what a schema change means
 * for the cookie set. Both interpreters are mechanical translations of what this
 * returns, in the order it returns them.
 *
 * Changes that carry no cookie state — index changes, and the `create-table` /
 * `add-column` variants whose optional `metadata` and `backfill` fields are
 * absent — produce no ops.
 */
export function cookieOps(change: SchemaChange): CookieOp[] {
  switch (change.tag) {
    case 'update-table-metadata':
      return [
        {op: 'upsert-metadata', table: change.table, metadata: change.new},
      ];

    case 'create-table': {
      const {spec, metadata, backfill} = change;
      const table = {schema: spec.schema, name: spec.name};
      const ops: CookieOp[] = [];
      if (metadata) {
        ops.push({op: 'upsert-metadata', table, metadata});
      }
      if (backfill) {
        for (const [column, id] of Object.entries(backfill)) {
          ops.push({op: 'upsert-backfill', table, column, backfill: id});
        }
      }
      return ops;
    }

    case 'rename-table':
      return [{op: 'rename-table', old: change.old, new: change.new}];

    case 'drop-table':
      return [{op: 'drop-table', table: change.id}];

    case 'add-column': {
      const {table, tableMetadata, column, backfill} = change;
      const ops: CookieOp[] = [];
      if (tableMetadata) {
        ops.push({op: 'upsert-metadata', table, metadata: tableMetadata});
      }
      if (backfill) {
        ops.push({
          op: 'upsert-backfill',
          table,
          column: column.name,
          backfill,
        });
      }
      return ops;
    }

    case 'update-column': {
      // Only a rename moves the cookie; the rest of a column update does not
      // affect whether or how it is being backfilled.
      const {table, old, new: updated} = change;
      return old.name === updated.name
        ? []
        : [{op: 'rename-column', table, old: old.name, new: updated.name}];
    }

    case 'drop-column':
      return [{op: 'drop-column', table: change.table, column: change.column}];

    case 'backfill-completed': {
      const {relation, columns} = change;
      // The rowKey columns are excluded from `columns` but are backfilled with
      // them, so both are cleared.
      const cleared = [...relation.rowKey.columns, ...columns];
      return cleared.length === 0
        ? []
        : [
            {
              op: 'complete-backfill',
              table: {schema: relation.schema, name: relation.name},
              columns: cleared,
            },
          ];
    }

    case 'create-index':
    case 'drop-index':
      return [];

    default:
      // Unreachable: `isSchemaChange()` gates on the same tag set this switch
      // covers. It is exhaustive so that a new schema-change tag is a compile
      // error here rather than a silently dropped backfill.
      unreachable(change);
  }
}

/**
 * The SQLite interpreter of {@link cookieOps}, over prepared statements on the
 * change-log database.
 *
 * The statements run inside the log's transaction, in stream order, at the point
 * the schema change is appended (see `ChangeLogStreamWriter`). That is what
 * makes the cookie set atomic with the head — a crash cannot leave cookies ahead
 * of or behind the rows they were folded from — and what makes an aborted
 * transaction discard its cookies with its rows, for free.
 *
 * Statements are prepared against the tables as they exist now, so an instance
 * does not survive a reconcile that drops and recreates them.
 */
export class ChangeLogCookieWriter {
  readonly #upsertMetadata: Statement;
  readonly #upsertBackfill: Statement;
  readonly #renameMetadataTable: Statement;
  readonly #renameBackfillTable: Statement;
  readonly #deleteMetadataTable: Statement;
  readonly #deleteBackfillTable: Statement;
  readonly #renameBackfillColumn: Statement;
  readonly #deleteBackfillColumn: Statement;

  constructor(db: Database) {
    this.#upsertMetadata = db.prepare(/*sql*/ `
      INSERT INTO "${CHANGE_LOG_TABLE_METADATA_TABLE}"
        ("schema", "table", "metadata") VALUES (?, ?, ?)
        ON CONFLICT ("schema", "table")
        DO UPDATE SET "metadata" = excluded."metadata"
    `);
    this.#upsertBackfill = db.prepare(/*sql*/ `
      INSERT INTO "${CHANGE_LOG_BACKFILLING_TABLE}"
        ("schema", "table", "column", "backfill") VALUES (?, ?, ?, ?)
        ON CONFLICT ("schema", "table", "column")
        DO UPDATE SET "backfill" = excluded."backfill"
    `);
    this.#renameMetadataTable = db.prepare(/*sql*/ `
      UPDATE "${CHANGE_LOG_TABLE_METADATA_TABLE}"
        SET "schema" = ?, "table" = ? WHERE "schema" = ? AND "table" = ?
    `);
    this.#renameBackfillTable = db.prepare(/*sql*/ `
      UPDATE "${CHANGE_LOG_BACKFILLING_TABLE}"
        SET "schema" = ?, "table" = ? WHERE "schema" = ? AND "table" = ?
    `);
    this.#deleteMetadataTable = db.prepare(/*sql*/ `
      DELETE FROM "${CHANGE_LOG_TABLE_METADATA_TABLE}"
        WHERE "schema" = ? AND "table" = ?
    `);
    this.#deleteBackfillTable = db.prepare(/*sql*/ `
      DELETE FROM "${CHANGE_LOG_BACKFILLING_TABLE}"
        WHERE "schema" = ? AND "table" = ?
    `);
    this.#renameBackfillColumn = db.prepare(/*sql*/ `
      UPDATE "${CHANGE_LOG_BACKFILLING_TABLE}" SET "column" = ?
        WHERE "schema" = ? AND "table" = ? AND "column" = ?
    `);
    this.#deleteBackfillColumn = db.prepare(/*sql*/ `
      DELETE FROM "${CHANGE_LOG_BACKFILLING_TABLE}"
        WHERE "schema" = ? AND "table" = ? AND "column" = ?
    `);
  }

  /** Applies the change's cookie ops, returning the ops that were applied. */
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
        this.#upsertMetadata.run(
          op.table.schema,
          op.table.name,
          BigIntJSON.stringify(op.metadata),
        );
        break;

      case 'upsert-backfill':
        this.#upsertBackfill.run(
          op.table.schema,
          op.table.name,
          op.column,
          BigIntJSON.stringify(op.backfill),
        );
        break;

      case 'rename-table':
        this.#renameMetadataTable.run(
          op.new.schema,
          op.new.name,
          op.old.schema,
          op.old.name,
        );
        this.#renameBackfillTable.run(
          op.new.schema,
          op.new.name,
          op.old.schema,
          op.old.name,
        );
        break;

      case 'drop-table':
        this.#deleteMetadataTable.run(op.table.schema, op.table.name);
        this.#deleteBackfillTable.run(op.table.schema, op.table.name);
        break;

      case 'rename-column':
        this.#renameBackfillColumn.run(
          op.new,
          op.table.schema,
          op.table.name,
          op.old,
        );
        break;

      case 'drop-column':
        this.#deleteBackfillColumn.run(
          op.table.schema,
          op.table.name,
          op.column,
        );
        break;

      case 'complete-backfill':
        // A per-column delete rather than an `IN` list, which cannot be a
        // single prepared statement across arities. The columns of one
        // completed backfill are few, and this runs only on schema changes.
        for (const column of op.columns) {
          this.#deleteBackfillColumn.run(
            op.table.schema,
            op.table.name,
            column,
          );
        }
        break;

      default:
        unreachable(op);
    }
  }
}

const tableKey = (schema: string, table: string) =>
  JSON.stringify([schema, table]);

const columnKey = (schema: string, table: string, column: string) =>
  JSON.stringify([schema, table, column]);

const cmp = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/**
 * The in-memory interpreter of {@link cookieOps}: the third one, and the only
 * one that is not a store.
 *
 * A replica-derived cookie set is a snapshot at the replica's state version,
 * which trails the change log's head, so it cannot be compared with a
 * Postgres-derived set directly. This carries it forward over the schema changes
 * the log holds in between, which turns "the two stores disagree, but they are
 * also at different positions" into an exact equality — and makes a
 * disagreement between any two of the three interpreters visible as an
 * inequality that neither store would have noticed on its own.
 *
 * `changes` must be the log's schema changes over `(from, to]` in stream order.
 * Anything else produces a set that belongs to no watermark.
 */
export function foldCookies(
  cookies: CookieSet,
  changes: Iterable<SchemaChange>,
): CookieSet {
  const metadata = new Map<string, TableMetadataCookie>(
    cookies.tableMetadata.map(c => [tableKey(c.schema, c.table), c]),
  );
  const backfilling = new Map<string, BackfillCookie>(
    cookies.backfilling.map(c => [columnKey(c.schema, c.table, c.column), c]),
  );
  const dropColumn = ({schema, name}: Identifier, column: string) =>
    backfilling.delete(columnKey(schema, name, column));

  for (const change of changes) {
    for (const op of cookieOps(change)) {
      switch (op.op) {
        case 'upsert-metadata':
          metadata.set(tableKey(op.table.schema, op.table.name), {
            schema: op.table.schema,
            table: op.table.name,
            metadata: op.metadata,
          });
          break;

        case 'upsert-backfill':
          backfilling.set(
            columnKey(op.table.schema, op.table.name, op.column),
            {
              schema: op.table.schema,
              table: op.table.name,
              column: op.column,
              backfill: op.backfill,
            },
          );
          break;

        case 'rename-table': {
          const {old, new: renamed} = op;
          const moved = metadata.get(tableKey(old.schema, old.name));
          if (moved) {
            metadata.delete(tableKey(old.schema, old.name));
            metadata.set(tableKey(renamed.schema, renamed.name), {
              ...moved,
              schema: renamed.schema,
              table: renamed.name,
            });
          }
          // Snapshotted before mutating, since the rename both deletes and
          // inserts into the map being scanned.
          for (const [key, cookie] of [...backfilling]) {
            if (cookie.schema === old.schema && cookie.table === old.name) {
              backfilling.delete(key);
              backfilling.set(
                columnKey(renamed.schema, renamed.name, cookie.column),
                {...cookie, schema: renamed.schema, table: renamed.name},
              );
            }
          }
          break;
        }

        case 'drop-table': {
          const {schema, name} = op.table;
          metadata.delete(tableKey(schema, name));
          for (const [key, cookie] of [...backfilling]) {
            if (cookie.schema === schema && cookie.table === name) {
              backfilling.delete(key);
            }
          }
          break;
        }

        case 'rename-column': {
          const {schema, name} = op.table;
          const moved = backfilling.get(columnKey(schema, name, op.old));
          if (moved) {
            backfilling.delete(columnKey(schema, name, op.old));
            backfilling.set(columnKey(schema, name, op.new), {
              ...moved,
              column: op.new,
            });
          }
          break;
        }

        case 'drop-column':
          dropColumn(op.table, op.column);
          break;

        case 'complete-backfill':
          for (const column of op.columns) {
            dropColumn(op.table, column);
          }
          break;

        default:
          unreachable(op);
      }
    }
  }

  return {
    tableMetadata: [...metadata.values()].toSorted(
      (a, b) => cmp(a.schema, b.schema) || cmp(a.table, b.table),
    ),
    backfilling: [...backfilling.values()].toSorted(
      (a, b) =>
        cmp(a.schema, b.schema) ||
        cmp(a.table, b.table) ||
        cmp(a.column, b.column),
    ),
  };
}

const backfillRequestsSchema = v.array(backfillRequestSchema);

/**
 * Returns one {@link BackfillRequest} for each table with an active backfill.
 * Each request contains the stored table metadata, if available.
 */
export function backfillRequestsFrom(cookies: CookieSet): BackfillRequest[] {
  const metadata = new Map(
    cookies.tableMetadata.map(c => [tableKey(c.schema, c.table), c.metadata]),
  );
  const requests: BackfillRequest[] = [];
  let curr: BackfillRequest | undefined;
  for (const {schema, table, column, backfill} of cookies.backfilling) {
    if (curr?.table.schema !== schema || curr.table.name !== table) {
      curr = {
        table: {
          schema,
          name: table,
          // Use `null` when a backfilling table has no metadata. This matches
          // the result of the Postgres left join.
          metadata: metadata.get(tableKey(schema, table)) ?? null,
        },
        columns: {},
      };
      requests.push(curr);
    }
    curr.columns[column] = backfill;
  }
  return v.parse(requests, backfillRequestsSchema);
}

/**
 * Reads the whole cookie set, ordered by primary key. This is what a
 * change-streamer would hand `startStream`, and what the Postgres-derived and
 * replica-derived sets are compared against.
 */
export function readCookies(db: Database): CookieSet {
  const tableMetadata = db
    .prepare(/*sql*/ `
      SELECT "schema", "table", "metadata"
        FROM "${CHANGE_LOG_TABLE_METADATA_TABLE}"
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
        FROM "${CHANGE_LOG_BACKFILLING_TABLE}"
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
 * Replaces the cookie set wholesale.
 *
 * The cookies are unversioned point-in-time state, so a reconciliation that
 * moves the head invalidates them: truncating the stream above `W` does not roll
 * the cookie set back to `W`, and the truncated transactions can contain
 * `create-table`, `add-column`, and `backfill-completed`. The caller must run
 * this in the same transaction as the reconciliation that invalidated them.
 */
export function replaceCookies(db: Database, cookies: CookieSet): void {
  db.prepare(/*sql*/ `DELETE FROM "${CHANGE_LOG_TABLE_METADATA_TABLE}"`).run();
  db.prepare(/*sql*/ `DELETE FROM "${CHANGE_LOG_BACKFILLING_TABLE}"`).run();

  if (cookies.tableMetadata.length > 0) {
    const insert = db.prepare(/*sql*/ `
      INSERT INTO "${CHANGE_LOG_TABLE_METADATA_TABLE}"
        ("schema", "table", "metadata") VALUES (?, ?, ?)
    `);
    for (const {schema, table, metadata} of cookies.tableMetadata) {
      insert.run(schema, table, BigIntJSON.stringify(metadata));
    }
  }
  if (cookies.backfilling.length > 0) {
    const insert = db.prepare(/*sql*/ `
      INSERT INTO "${CHANGE_LOG_BACKFILLING_TABLE}"
        ("schema", "table", "column", "backfill") VALUES (?, ?, ?, ?)
    `);
    for (const {schema, table, column, backfill} of cookies.backfilling) {
      insert.run(schema, table, column, BigIntJSON.stringify(backfill));
    }
  }
}

/** Retained cookie rows, by table. Both are normally zero. */
export type CookieRowCounts = {
  readonly tableMetadata: number;
  readonly backfilling: number;
};

export function readCookieRowCounts(db: Database): CookieRowCounts {
  return db
    .prepare(/*sql*/ `
      SELECT
        (SELECT count(*) FROM "${CHANGE_LOG_TABLE_METADATA_TABLE}")
          AS "tableMetadata",
        (SELECT count(*) FROM "${CHANGE_LOG_BACKFILLING_TABLE}")
          AS "backfilling"
    `)
    .get<CookieRowCounts>();
}
