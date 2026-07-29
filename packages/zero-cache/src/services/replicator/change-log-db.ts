/**
 * The change log lives in its own SQLite database, `${replicaFile}-change-log`,
 * rather than in the replica file. It is a catchup cache: everything it holds is
 * either already durable in the replica's litestream backup or re-derivable from
 * the upstream replication slot, so it is deliberately excluded from the backup
 * and is never a source of truth.
 *
 * Because it is disposable, it has no migration framework. Any inconsistency
 * with the replica — a schema change, a leftover file from a different replica,
 * a gap left by a crash or by running with the writer disabled — is resolved by
 * {@link reconcileChangeLog}, which either truncates the offending rows or wipes
 * and reseeds the log at the replica's current head. The worst outcome is a
 * `too-old` for a lagging subscriber; a silently delivered gap is not reachable.
 */

import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {deleteLiteDB} from '../../db/delete-lite-db.ts';
import {
  CHANGE_LOG_STREAM_TABLE,
  CREATE_CHANGE_LOG_STREAM_SCHEMA,
  SEED_CHANGE_LOG_STREAM_SQL,
} from './schema/change-log-stream.ts';

/**
 * Bumped whenever the change-log database's schema changes. Since the file is a
 * cache, a mismatch costs one reseed rather than a migration.
 */
export const CHANGE_LOG_DB_SCHEMA_VERSION = 1;

export const CHANGE_LOG_META_TABLE = '_zero.changeLogMeta';

// "replicaVersion" identifies the replica this log was built against, which
// detects a file left behind by a replica that was since reset and re-synced.
// "lock" enforces single-row semantics, as in "_zero.replicationConfig".
const CREATE_CHANGE_LOG_META_SCHEMA = /*sql*/ `
  CREATE TABLE "${CHANGE_LOG_META_TABLE}" (
    "replicaVersion" TEXT NOT NULL,
    "schemaVersion"  INTEGER NOT NULL,
    "lock"           INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
  );
`;

/**
 * `${replicaFile}-change-log`, matching the `-serving-copy` convention.
 *
 * Its `-wal` / `-wal2` / `-shm` sidecars are suffixes of *this* name, so they
 * never collide with the replica's own sidecars.
 */
export function changeLogFileName(replicaFile: string): string {
  return `${replicaFile}-change-log`;
}

/**
 * Removes the change-log database and its `-wal` / `-wal2` / `-shm` sidecars.
 * Safe to call when the file does not exist.
 */
export function deleteChangeLogDB(replicaFile: string): void {
  deleteLiteDB(changeLogFileName(replicaFile));
}

/** The replica facts the change log is anchored to. */
export type ReplicaAnchor = {
  readonly replicaVersion: string;
  readonly stateVersion: string;
  readonly writeTimeMs: number;
};

type ReplicaAnchorRow = {
  readonly replicaVersion: string;
  readonly stateVersion: string;
  readonly writeTimeMs: number | null;
};

export function readReplicaAnchor(replica: Database): ReplicaAnchor {
  const row = replica
    .prepare(/*sql*/ `
      SELECT config."replicaVersion",
             state."stateVersion",
             state."writeTimeMs"
        FROM "_zero.replicationConfig" AS config,
             "_zero.replicationState" AS state
    `)
    .get<ReplicaAnchorRow | undefined>();
  assert(row !== undefined, 'replication state must be initialized');
  assert(
    row.writeTimeMs !== null,
    'replication state writeTimeMs must be initialized',
  );
  return {
    replicaVersion: row.replicaVersion,
    stateVersion: row.stateVersion,
    writeTimeMs: row.writeTimeMs,
  };
}

/**
 * Opens the change-log database beside `replicaFile`.
 *
 * A writable handle creates the file if it is absent; a `readonly` handle
 * throws, which is the normal state for a change-streamer that starts before
 * the replicator has created the log. Pragmas are the caller's responsibility,
 * since only the writer sets the persistent ones.
 */
export function openChangeLogDB(
  lc: LogContext,
  replicaFile: string,
  opts: {readonly: boolean},
): Database {
  return new Database(
    lc.withContext('component', 'change-log-db'),
    changeLogFileName(replicaFile),
    {readonly: opts.readonly},
  );
}

/**
 * Configures the change-log database, which the write worker owns exclusively.
 * These are the settings the `separate-files` mode of
 * `sqlite-change-log-ceiling.bench.ts` measured.
 *
 * They deliberately do not go through `getPragmaConfig` in `workers/replicator`
 * beside the replica's, because the write worker applies them in its own
 * thread and that module reaches the Postgres client through the replica
 * migrations.
 *
 * `wal2` because the log is a continuous append that catchup reads
 * continuously, which is the case wal2 exists for: it avoids checkpoint
 * starvation without needing an autocheckpoint. `wal_autocheckpoint` therefore
 * keeps its default, unlike the backup replica's 0, which hands checkpointing
 * to litestream — nothing else owns this file.
 *
 * `synchronous = NORMAL` because the log commits *before* the replica on the
 * replication hot path, and NORMAL keeps that from costing a second fsync. It
 * is not a durability compromise here: in WAL mode NORMAL still writes every
 * commit through to the OS, so it survives a process crash, an OOM kill, or a
 * SIGKILL. It is lost only to kernel panic or power loss, and those destroy the
 * node — the task restarts elsewhere, restores the replica from S3, and has no
 * change-log file at all. Whatever a power loss does take is detected as a gap
 * by {@link reconcileChangeLog} and reseeded.
 */
export function applyChangeLogPragmas(db: Database): void {
  db.pragma('busy_timeout = 30000');
  db.pragma('analysis_limit = 1000');
  db.pragma('journal_mode = wal2');
  db.pragma('synchronous = NORMAL');
}

export type ReseedReason =
  | 'created' // file or table absent
  | 'schema-mismatch'
  | 'replica-version-mismatch'
  | 'gap'; // head < stateVersion after truncation

export type ReconcileResult =
  | {action: 'none'; head: string}
  | {action: 'truncated'; head: string; rows: number}
  | {action: 'reseeded'; head: string; reason: ReseedReason};

/**
 * Brings the change log into agreement with the replica, in one transaction on
 * the change-log database.
 *
 * Writes are ordered log-first, so a crash can leave a *phantom* — a
 * transaction in the log whose replica commit was lost — but never a *hole*.
 * A phantom's rows all carry the aborted transaction's commit watermark, which
 * is strictly greater than the replica's `stateVersion`, so deleting on that
 * predicate removes phantoms whole and retains every committed transaction.
 *
 * Anything the ordering guarantee does not cover — the file was deleted, the
 * replica ran with the writer disabled, the replica was restored, power was
 * lost with `synchronous=NORMAL` — surfaces as a head that does not land on
 * `stateVersion` after truncation, and is resolved by wiping and reseeding at
 * the replica head.
 */
export function reconcileChangeLog(
  lc: LogContext,
  db: Database,
  anchor: ReplicaAnchor,
): ReconcileResult {
  db.exec('BEGIN IMMEDIATE');
  try {
    const result = reconcile(lc, db, anchor);
    db.exec('COMMIT');
    return result;
  } catch (e) {
    if (db.inTransaction) {
      db.exec('ROLLBACK');
    }
    throw e;
  }
}

function reconcile(
  lc: LogContext,
  db: Database,
  anchor: ReplicaAnchor,
): ReconcileResult {
  const wipe = wipeReason(db, anchor);
  if (wipe !== undefined) {
    return reseed(lc, db, anchor, wipe);
  }

  let head = readHead(db);
  let rows = 0;
  if (head !== null && head > anchor.stateVersion) {
    rows = db
      .prepare(/*sql*/ `
        DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" > ?
      `)
      .run(anchor.stateVersion).changes;
    head = readHead(db);
  }

  if (head !== anchor.stateVersion) {
    return reseed(lc, db, anchor, 'gap');
  }
  if (rows > 0) {
    lc.info?.('truncated phantom transactions from the SQLite change log', {
      sqliteChangeLogReconcile: {head, rows},
    });
    return {action: 'truncated', head, rows};
  }
  lc.debug?.('SQLite change log is consistent with the replica', {
    sqliteChangeLogReconcile: {head},
  });
  return {action: 'none', head};
}

function wipeReason(
  db: Database,
  anchor: ReplicaAnchor,
): ReseedReason | undefined {
  if (
    !tableExists(db, CHANGE_LOG_STREAM_TABLE) ||
    !tableExists(db, CHANGE_LOG_META_TABLE)
  ) {
    return 'created';
  }
  const meta = db
    .prepare(/*sql*/ `
      SELECT "replicaVersion", "schemaVersion" FROM "${CHANGE_LOG_META_TABLE}"
    `)
    .get<{replicaVersion: string; schemaVersion: number} | undefined>();
  if (meta === undefined) {
    return 'created';
  }
  if (meta.schemaVersion !== CHANGE_LOG_DB_SCHEMA_VERSION) {
    return 'schema-mismatch';
  }
  if (meta.replicaVersion !== anchor.replicaVersion) {
    return 'replica-version-mismatch';
  }
  return undefined;
}

function reseed(
  lc: LogContext,
  db: Database,
  anchor: ReplicaAnchor,
  reason: ReseedReason,
): ReconcileResult {
  // Dropping the table drops its partial index with it.
  db.exec(/*sql*/ `
    DROP TABLE IF EXISTS "${CHANGE_LOG_STREAM_TABLE}";
    DROP TABLE IF EXISTS "${CHANGE_LOG_META_TABLE}";
    ${CREATE_CHANGE_LOG_STREAM_SCHEMA}
    ${CREATE_CHANGE_LOG_META_SCHEMA}
  `);
  db.prepare(/*sql*/ `
    INSERT INTO "${CHANGE_LOG_META_TABLE}" ("replicaVersion", "schemaVersion")
      VALUES (?, ?)
  `).run(anchor.replicaVersion, CHANGE_LOG_DB_SCHEMA_VERSION);
  seedChangeLogStream(db, anchor);

  lc.info?.('reseeded the SQLite change log', {
    sqliteChangeLogReconcile: {
      reason,
      head: anchor.stateVersion,
      replicaVersion: anchor.replicaVersion,
      schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
    },
  });
  return {action: 'reseeded', head: anchor.stateVersion, reason};
}

/**
 * Seeds a valid synthetic transaction at the replica's current watermark,
 * making an otherwise empty log serviceable as a catchup boundary for a
 * subscriber that is exactly at the replica head.
 */
export function seedChangeLogStream(db: Database, anchor: ReplicaAnchor): void {
  db.prepare(SEED_CHANGE_LOG_STREAM_SQL).run({
    stateVersion: anchor.stateVersion,
    writeTimeMs: anchor.writeTimeMs,
  });
}

/**
 * `max()` on the primary key's leading column, i.e. an index seek. Null when
 * the log is empty.
 */
function readHead(db: Database): string | null {
  const {head} = db
    .prepare(/*sql*/ `
      SELECT max("watermark") AS "head" FROM "${CHANGE_LOG_STREAM_TABLE}"
    `)
    .get<{head: string | null}>();
  return head;
}

function tableExists(db: Database, table: string): boolean {
  return (
    db
      .prepare(/*sql*/ `
        SELECT 1 FROM "sqlite_master" WHERE "type" = 'table' AND "name" = ?
      `)
      .get<{1: number} | undefined>(table) !== undefined
  );
}
