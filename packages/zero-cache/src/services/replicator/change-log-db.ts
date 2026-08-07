/**
 * The change log lives in its own SQLite database, `${replicaFile}-change-log`,
 * rather than in the replica file. It is a catchup cache: everything it holds is
 * either already durable in the replica's litestream backup or re-derivable from
 * the upstream replication slot, so it is deliberately excluded from the backup
 * and is never a source of truth.
 *
 * The change-streamer writes it, from the stream loop that fans changes out to
 * subscribers, and commits it before forwarding each transaction's `commit`
 * message. Its head is therefore always at or above the watermark the stream
 * would resume from, which is what {@link reconcileChangeLog} anchors on.
 *
 * Beside the buffer it holds the log's *cookie jar* — the backfill progress
 * state folded up to the same head. That is `change-log-cookies.ts`; this file
 * owns its tables' lifecycle, because a cookie set is only meaningful paired
 * with the watermark it was folded to, and so must be created, dropped, and
 * invalidated with the buffer rather than beside it.
 *
 * Because it is disposable, it has no migration framework. Any inconsistency —
 * a schema change, a leftover file from a different replica, a gap left by a
 * crash or by running with the writer disabled, a corrupt file — is resolved by
 * {@link reconcileChangeLog} and {@link openChangeLogDBForWriting}, which
 * truncate the offending rows or wipe and reseed the log at the resume
 * watermark. The worst outcome is a `too-old` for a lagging subscriber; a
 * silently delivered gap is not reachable.
 *
 * Disk sizing: a task that writes the log needs room for the replica *plus* the
 * retained change stream — the log's main file, its wal2 sidecars, and pages a
 * purge has freed but not yet returned to the OS. The change-streamer schedules
 * purges against the same floor as the PG change log, so retention is bounded
 * by the confirmed backup watermark ANDed with the minimum retention window —
 * a function of the backup interval, not a constant: 15–30+ minutes behind an
 * RMv1 sync interval, ~60 seconds once litestream v5 backs up every 15–30 s.
 * A held snapshot reservation pins retention unboundedly for its duration, so
 * peak retained bytes is set by the slowest concurrent restore rather than by
 * the retention setting — size disk and alerts on that. The log is excluded
 * from the litestream backup, so this is local disk only and never S3.
 */

import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../zqlite/src/db.ts';
import {deleteLiteDB} from '../../db/delete-lite-db.ts';
import {
  isSQLiteCorruption,
  logSQLiteCorruptionDiagnostics,
} from '../../db/sqlite-corruption.ts';
import {
  CHANGE_LOG_BACKFILLING_TABLE,
  CHANGE_LOG_TABLE_METADATA_TABLE,
  CREATE_CHANGE_LOG_COOKIE_SCHEMA,
  DROP_CHANGE_LOG_COOKIE_TABLES,
  EMPTY_COOKIE_SET,
  replaceCookies,
} from './change-log-cookies.ts';

/**
 * Bumped whenever the change-log database's schema changes. Since the file is a
 * cache, a mismatch costs one reseed rather than a migration.
 *
 * v2 re-homed the writer to the change-streamer: the log anchors on the
 * watermark its stream connection resumes from rather than on the replica's
 * state version, and its meta row carries the replica's identity triple plus
 * the seed point.
 *
 * v3 added the cookie tables (see `change-log-cookies.ts`), i.e. the log's
 * second job. The reseed it costs on upgrade buys one retention window of
 * catchup reach on a log nothing reads yet.
 *
 * `auto_vacuum = INCREMENTAL` arrived within v2, deliberately without a bump:
 * it is a file-header property rather than a schema change, and the reseed a
 * version mismatch triggers cannot enable it (see
 * {@link applyChangeLogPragmas}), so {@link openChangeLogDBForWriting} guards
 * on the pragma itself instead of on this number.
 */
export const CHANGE_LOG_DB_SCHEMA_VERSION = 3;

/** https://www.sqlite.org/pragma.html#pragma_auto_vacuum */
const AUTO_VACUUM_INCREMENTAL = 2;

export const CHANGE_LOG_STREAM_TABLE = '_zero.changeLogStream';
export const CHANGE_LOG_STREAM_WRITE_TIME_INDEX =
  '_zero.changeLogStream_writeTimeMs';

// The index is partial because only commit rows carry a "writeTimeMs", so it
// holds one entry per transaction rather than one per change. It is what the
// purger scans to find transactions older than the retention window.
//
// The replica carried this same table through schema versions 14 and 15; see
// `CREATE_V14_CHANGE_LOG_STREAM` in `change-source/common/replica-schema.ts`,
// which is frozen at the v14 shape and must not be kept in sync with this one.
export const CREATE_CHANGE_LOG_STREAM_SCHEMA = /*sql*/ `
  CREATE TABLE "${CHANGE_LOG_STREAM_TABLE}" (
    "watermark"   TEXT NOT NULL,
    "pos"         INTEGER NOT NULL,
    "change"      TEXT NOT NULL,
    "precommit"   TEXT,
    "writeTimeMs" INTEGER,
    PRIMARY KEY ("watermark", "pos")
  );

  CREATE INDEX "${CHANGE_LOG_STREAM_WRITE_TIME_INDEX}"
    ON "${CHANGE_LOG_STREAM_TABLE}" ("writeTimeMs", "watermark")
    WHERE "writeTimeMs" IS NOT NULL;
`;

/**
 * Inserts the synthetic begin/commit pair for the seed watermark.
 *
 * Both rows are inserted by one statement so a caller outside a larger
 * transaction cannot leave a partial seed.
 */
const SEED_CHANGE_LOG_STREAM_SQL = /*sql*/ `
  INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
    ("watermark", "pos", "change", "precommit", "writeTimeMs")
  VALUES
    (@watermark, 0, '{"tag":"begin"}', NULL, NULL),
    (@watermark, 1, '{"tag":"commit"}', @watermark, @writeTimeMs)
  ON CONFLICT ("watermark", "pos") DO NOTHING
`;

export const CHANGE_LOG_META_TABLE = '_zero.changeLogMeta';

// The identity triple (see ChangeLogIdentity) detects a file left behind by a
// different replica -- most concretely, a leftover log on a reused volume.
//
// "seededAtMs"    : when the log was seeded, i.e. how far its retention window
//                   can possibly reach back. Slice 11 uses it as the warm-up
//                   signal for canary reads.
// "seedWatermark" : where the log was seeded. `min("watermark")` cannot answer
//                   this once purge has advanced the minimum, and the
//                   difference is what distinguishes a log that purged past the
//                   retention floor (an invariant violation) from one that
//                   never held that history at all (routine at every fork).
// "lock"          : enforces single-row semantics, as in
//                   "_zero.replicationConfig".
const CREATE_CHANGE_LOG_META_SCHEMA = /*sql*/ `
  CREATE TABLE "${CHANGE_LOG_META_TABLE}" (
    "epoch"         TEXT,
    "generation"    TEXT NOT NULL,
    "replicaID"     TEXT,
    "schemaVersion" INTEGER NOT NULL,
    "seededAtMs"    INTEGER NOT NULL,
    "seedWatermark" TEXT NOT NULL,
    "lock"          INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
  );
`;

/**
 * `${replicaFile}-change-log`, matching the `-serving-copy` convention.
 *
 * The name outlives the writer's move into the change-streamer: the
 * change-streamer derives the path from the replica file it owns, so renaming
 * would save no plumbing and would orphan the old file on every existing
 * volume, where nothing would delete it.
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

/**
 * Which replica the log's contents belong to.
 *
 * `generation` is RMv1's `replicaVersion`: the version at which initial sync
 * copied every row. It is shared by every sibling of a forked replica, which is
 * why it cannot be the whole of the identity under RMv2 — `epoch` and
 * `replicaID` are what distinguish two siblings' logs. Both are `null` until
 * RMv2 supplies them, and a `null` matches only another `null`, so a log
 * written before they exist is still recognized as this replica's.
 */
export type ChangeLogIdentity = {
  readonly epoch: string | null;
  readonly generation: string;
  readonly replicaID: string | null;
};

/**
 * What the log is reconciled against. Reconciliation runs once per stream
 * connection, since every reconnect can re-read a different resume watermark.
 */
export type ChangeLogAnchor = {
  readonly identity: ChangeLogIdentity;
  /**
   * The watermark this stream connection resumes from. The log's own head is
   * measured against it, and a reseed seeds a synthetic transaction at it.
   *
   * Deliberately *not* "the replica's state version": after the writer moved to
   * the change-streamer, the canonical replicator is a subscriber downstream of
   * the write loop, so its state version is a consequence of this log rather
   * than a constraint on it.
   */
  readonly resumeWatermark: string;
  /**
   * The change-streamer's clock, at reconciliation time. Supplied rather than
   * read inline so that tests control it. Used for `seededAtMs` and for the
   * seed transaction's `writeTimeMs`, which is what the purger's time floor
   * measures.
   */
  readonly nowMs: number;
};

/** The meta row, i.e. everything the log records about itself. */
export type ChangeLogMeta = ChangeLogIdentity & {
  readonly schemaVersion: number;
  readonly seededAtMs: number;
  readonly seedWatermark: string;
};

/**
 * Opens the change-log database beside `replicaFile`.
 *
 * A writable handle creates the file if it is absent; a `readonly` handle
 * throws, which is the normal state for a change-streamer whose writer is
 * disabled. Pragmas are the caller's responsibility, since only the writer sets
 * the persistent ones.
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
 * Configures the change-log database, which the change-streamer's writer owns
 * exclusively. These are the settings the `separate-files` mode of
 * `sqlite-change-log-ceiling.bench.ts` measured.
 *
 * `wal2` because the log is a continuous append that catchup reads
 * continuously, which is the case wal2 exists for: it avoids checkpoint
 * starvation without needing an autocheckpoint. `wal_autocheckpoint` therefore
 * keeps its default, unlike the backup replica's 0, which hands checkpointing
 * to litestream — nothing else owns this file.
 *
 * `synchronous = NORMAL` because the commit sits on the forward path, once per
 * upstream transaction, and NORMAL keeps that from costing an fsync. It is not
 * a durability compromise here: in WAL mode NORMAL still writes every commit
 * through to the OS, so it survives a process crash, an OOM kill, or a SIGKILL.
 * It is lost only to kernel panic or power loss, and those destroy the node —
 * the task restarts elsewhere, restores the replica from S3, and has no
 * change-log file at all. Whatever a power loss does take is detected by
 * {@link reconcileChangeLog} and reseeded.
 *
 * `auto_vacuum = INCREMENTAL` so that purge can return freed pages to the OS
 * rather than leaving the file at its high-water mark. **It must be the first
 * statement here.** SQLite honours a `none` -> `incremental` transition only
 * while the file is still empty, and `journal_mode = wal2` materializes page 1;
 * appending the pragma instead of prepending it silently leaves the file at
 * mode 0 forever, since neither `DROP TABLE` nor a reseed makes a file empty
 * again. {@link openChangeLogDBForWriting} verifies the result.
 */
export function applyChangeLogPragmas(db: Database): void {
  db.pragma('auto_vacuum = INCREMENTAL');
  db.pragma('busy_timeout = 30000');
  db.pragma('analysis_limit = 1000');
  db.pragma('journal_mode = wal2');
  db.pragma('synchronous = NORMAL');
}

function readAutoVacuum(db: Database): number {
  const [{auto_vacuum: mode}] = db.pragma<{auto_vacuum: number}>('auto_vacuum');
  return mode;
}

/**
 * Opens the change-log database for the writer, applies its pragmas, and
 * reconciles it against `anchor`, returning the handle and what reconciliation
 * had to do.
 *
 * Corruption is answered with a rebuild rather than an escalation, which is a
 * deliberate divergence from how the replica's own corruption is handled. A
 * corrupt replica is a lost source of truth, so it ends in a restore or an
 * auto-reset; the change log holds nothing that is not either already in the
 * replica's litestream backup or re-derivable from the upstream slot, so the
 * entire response is to delete the file and seed a new one at the resume
 * watermark. The cost is one retention window of catchup — reconnecting
 * subscribers below the head get `too-old` — which is what every other
 * {@link ReseedReason} costs.
 *
 * Only corruption is rebuilt through. Anything else (a bad path, a permissions
 * error, a full disk) is a problem with the environment rather than with the
 * file's contents, and deleting a database in response would destroy state
 * without fixing it. Those propagate to the caller, which disables the writer
 * and keeps replicating.
 *
 * A file that predates `auto_vacuum = INCREMENTAL` takes the same rebuild, for
 * the same reason and at the same cost: the pragma only takes on an empty file,
 * so a reseed cannot enable it. Unlike corruption, a *second* failure there is
 * tolerated rather than thrown — see {@link ensureIncrementalAutoVacuum}.
 */
export function openChangeLogDBForWriting(
  lc: LogContext,
  replicaFile: string,
  anchor: ChangeLogAnchor,
): {db: Database; result: ReconcileResult} {
  const opened = openOrRebuildCorrupt(lc, replicaFile, anchor);
  const {db, result} = ensureIncrementalAutoVacuum(
    lc,
    replicaFile,
    anchor,
    opened,
  );
  return {db, result};
}

function openOrRebuildCorrupt(
  lc: LogContext,
  replicaFile: string,
  anchor: ChangeLogAnchor,
): OpenedChangeLog {
  try {
    return openAndReconcile(lc, replicaFile, anchor);
  } catch (e) {
    if (!isSQLiteCorruption(e)) {
      throw e;
    }
    const file = changeLogFileName(replicaFile);
    logSQLiteCorruptionDiagnostics(lc, 'change-log', file, e);
    lc.error?.('rebuilding the corrupt SQLite change log', {
      sqliteChangeLog: {file},
    });
    return rebuildChangeLog(lc, replicaFile, anchor);
  }
}

/**
 * Rebuilds a log whose `auto_vacuum` mode is not `INCREMENTAL`, which is every
 * log created before the pragma shipped — including every v2 file the writer
 * wrote in the meantime, which is why the guard is on the pragma rather than
 * on the schema version. Deleting the file is the only way to enable it:
 * `DROP TABLE` leaves the header's mode intact, and the in-place alternative —
 * a full `VACUUM` — is an unbounded blocking statement on a file the writer
 * needs.
 *
 * A mode that is still wrong after the rebuild is logged and tolerated, which
 * is the one place this diverges from the corruption path. The two failures are
 * not alike: a file that stays corrupt after a rebuild is an environment
 * problem, but a pragma that stays wrong on a brand-new file can only mean
 * {@link applyChangeLogPragmas}' statement order is wrong — a code bug, present
 * on every task, on every start. Throwing there turns a lost optimization into
 * a crash loop, and rebuilding again turns it into a reseed per start, which is
 * worse than the freelist growth it was trying to avoid. `Database.compact()`
 * already warns and no-ops when the mode is not `INCREMENTAL`, so degrading is
 * the accepted fallback.
 */
function ensureIncrementalAutoVacuum(
  lc: LogContext,
  replicaFile: string,
  anchor: ChangeLogAnchor,
  opened: OpenedChangeLog,
): OpenedChangeLog {
  if (opened.autoVacuum === AUTO_VACUUM_INCREMENTAL) {
    return opened;
  }
  const file = changeLogFileName(replicaFile);
  lc.info?.('rebuilding the SQLite change log to enable incremental vacuum', {
    sqliteChangeLog: {file, autoVacuum: opened.autoVacuum},
  });
  opened.db.close();
  const rebuilt = rebuildChangeLog(lc, replicaFile, anchor);
  if (rebuilt.autoVacuum !== AUTO_VACUUM_INCREMENTAL) {
    lc.error?.(
      'SQLite change log does not support incremental vacuum; ' +
        'it will plateau at its high-water mark',
      {sqliteChangeLog: {file, autoVacuum: rebuilt.autoVacuum}},
    );
  }
  return rebuilt;
}

/** The shared "delete the file and open a new one" body. */
function rebuildChangeLog(
  lc: LogContext,
  replicaFile: string,
  anchor: ChangeLogAnchor,
): OpenedChangeLog {
  deleteChangeLogDB(replicaFile);
  // Reconciles a database that does not exist, i.e. reseeds with reason
  // 'created'. A second failure is the environment's, not the file's.
  return openAndReconcile(lc, replicaFile, anchor);
}

type OpenedChangeLog = {
  db: Database;
  result: ReconcileResult;
  /** The file's persistent `auto_vacuum` mode, read back after the pragmas. */
  autoVacuum: number;
};

function openAndReconcile(
  lc: LogContext,
  replicaFile: string,
  anchor: ChangeLogAnchor,
): OpenedChangeLog {
  const db = openChangeLogDB(lc, replicaFile, {readonly: false});
  try {
    applyChangeLogPragmas(db);
    const autoVacuum = readAutoVacuum(db);
    return {db, result: reconcileChangeLog(lc, db, anchor), autoVacuum};
  } catch (e) {
    // The caller never sees this handle, so it closes here or leaks — and it
    // must be closed before the corruption path deletes the file.
    db.close();
    throw e;
  }
}

export type ReseedReason =
  | 'created' // file or table absent
  | 'schema-mismatch'
  | 'identity-mismatch'
  | 'gap'; // head does not land on the resume watermark after truncation

/**
 * `cookiesStale` records that the cookie set the log held did not survive
 * reconciliation. The cookies are unversioned point-in-time state, so any
 * reconciliation that moves the head invalidates them, and the same transaction
 * clears them (see `change-log-cookies.ts`). It is `action !== 'none'` today;
 * it is a field rather than a derivation because the reseed that replaces the
 * clear reads as intent at the call site, and because it is what the
 * `cookie_reseed` counter is keyed on.
 */
export type ReconcileResult =
  | {action: 'none'; head: string; cookiesStale: false}
  | {action: 'truncated'; head: string; rows: number; cookiesStale: true}
  | {
      action: 'reseeded';
      head: string;
      reason: ReseedReason;
      cookiesStale: true;
    };

/**
 * Brings the change log into agreement with the watermark its stream connection
 * resumes from, in one transaction on the change-log database:
 *
 * ```
 * head === resumeWatermark  → keep appending
 * head  >  resumeWatermark  → truncate above resumeWatermark
 * otherwise                 → wipe, seed at resumeWatermark, record the reason
 * ```
 *
 * The log commits before anything that can advance the resume watermark, so a
 * *phantom* — a transaction the log holds whose watermark the resumed stream
 * re-delivers — is the normal state, and truncate-above is the whole of
 * reconciliation rather than a special case. A phantom's rows all carry the
 * transaction's commit watermark, which is strictly greater than the resume
 * watermark, so deleting on that predicate removes phantoms whole and retains
 * every transaction at or below the resume point.
 *
 * It must run per stream connection, not once per process: the stream loop
 * re-reads its resume watermark on every reconnect, and the writer inserts with
 * a plain `INSERT` against `PRIMARY KEY ("watermark","pos")`, so an
 * un-truncated overlap is a constraint violation on the first re-delivered
 * transaction.
 *
 * Anything truncation cannot resolve — the file was deleted, the writer was
 * disabled for a while, the replica was restored, power was lost with
 * `synchronous=NORMAL` — surfaces as a head that does not land on the resume
 * watermark, and is resolved by wiping and reseeding there.
 */
export function reconcileChangeLog(
  lc: LogContext,
  db: Database,
  anchor: ChangeLogAnchor,
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
  anchor: ChangeLogAnchor,
): ReconcileResult {
  const wipe = wipeReason(db, anchor);
  if (wipe !== undefined) {
    return reseed(lc, db, anchor, wipe);
  }

  const {resumeWatermark} = anchor;
  let head = readChangeLogHead(db);
  let rows = 0;
  if (head !== null && head > resumeWatermark) {
    rows = db
      .prepare(/*sql*/ `
        DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" > ?
      `)
      .run(resumeWatermark).changes;
    head = readChangeLogHead(db);
  }

  if (head !== resumeWatermark) {
    return reseed(lc, db, anchor, 'gap');
  }
  if (rows > 0) {
    // The truncated transactions can have carried `create-table`,
    // `add-column`, and `backfill-completed`, none of which the truncate rolls
    // back, so the cookie set no longer belongs to the head. Clearing it is
    // correct but lossy; the anchor supplies the set that belongs to the resume
    // watermark once there is one to supply. Nothing reads the cookies in the
    // meantime.
    replaceCookies(db, EMPTY_COOKIE_SET);
    lc.info?.('truncated phantom transactions from the SQLite change log', {
      sqliteChangeLogReconcile: {head, rows},
    });
    return {action: 'truncated', head, rows, cookiesStale: true};
  }
  lc.debug?.('SQLite change log is at the resume watermark', {
    sqliteChangeLogReconcile: {head},
  });
  return {action: 'none', head, cookiesStale: false};
}

function wipeReason(
  db: Database,
  anchor: ChangeLogAnchor,
): ReseedReason | undefined {
  if (
    !tableExists(db, CHANGE_LOG_STREAM_TABLE) ||
    !tableExists(db, CHANGE_LOG_META_TABLE)
  ) {
    return 'created';
  }
  // The schema version is read on its own because it is the one column every
  // version of this table has had. Reading the rest of a v1 row would fail on
  // its missing columns instead of reporting the mismatch that explains it.
  const version = db
    .prepare(/*sql*/ `
      SELECT "schemaVersion" FROM "${CHANGE_LOG_META_TABLE}"
    `)
    .get<{schemaVersion: number} | undefined>();
  if (version === undefined) {
    return 'created';
  }
  if (version.schemaVersion !== CHANGE_LOG_DB_SCHEMA_VERSION) {
    return 'schema-mismatch';
  }
  // Checked *after* the version, so that every pre-v3 file — which is every
  // file in the fleet at the upgrade — reports the `schema-mismatch` that
  // explains it rather than the `created` its absent cookie tables would
  // otherwise look like.
  if (
    !tableExists(db, CHANGE_LOG_TABLE_METADATA_TABLE) ||
    !tableExists(db, CHANGE_LOG_BACKFILLING_TABLE)
  ) {
    return 'created';
  }
  const {epoch, generation, replicaID} = readChangeLogMeta(db);
  const identity = anchor.identity;
  if (
    epoch !== identity.epoch ||
    generation !== identity.generation ||
    replicaID !== identity.replicaID
  ) {
    return 'identity-mismatch';
  }
  return undefined;
}

function reseed(
  lc: LogContext,
  db: Database,
  anchor: ChangeLogAnchor,
  reason: ReseedReason,
): ReconcileResult {
  const {identity, resumeWatermark, nowMs} = anchor;
  // Dropping the table drops its partial index with it. The cookie tables go
  // with the buffer: a cookie set without the transactions it was folded from
  // is not a smaller cookie set, it is a wrong one.
  db.exec(/*sql*/ `
    DROP TABLE IF EXISTS "${CHANGE_LOG_STREAM_TABLE}";
    DROP TABLE IF EXISTS "${CHANGE_LOG_META_TABLE}";
    ${DROP_CHANGE_LOG_COOKIE_TABLES}
    ${CREATE_CHANGE_LOG_STREAM_SCHEMA}
    ${CREATE_CHANGE_LOG_META_SCHEMA}
    ${CREATE_CHANGE_LOG_COOKIE_SCHEMA}
  `);
  db.prepare(/*sql*/ `
    INSERT INTO "${CHANGE_LOG_META_TABLE}"
      ("epoch", "generation", "replicaID", "schemaVersion", "seededAtMs",
       "seedWatermark")
      VALUES (@epoch, @generation, @replicaID, @schemaVersion, @seededAtMs,
              @seedWatermark)
  `).run({
    epoch: identity.epoch,
    generation: identity.generation,
    replicaID: identity.replicaID,
    schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
    seededAtMs: nowMs,
    seedWatermark: resumeWatermark,
  });
  seedChangeLogStream(db, anchor);

  lc.info?.('reseeded the SQLite change log', {
    sqliteChangeLogReconcile: {
      reason,
      head: resumeWatermark,
      ...identity,
      schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
      seededAtMs: nowMs,
    },
  });
  return {
    action: 'reseeded',
    head: resumeWatermark,
    reason,
    cookiesStale: true,
  };
}

/**
 * Seeds a valid synthetic transaction at the anchor's resume watermark, making
 * an otherwise empty log serviceable as a catchup boundary for a subscriber
 * that is exactly at that watermark.
 */
export function seedChangeLogStream(
  db: Database,
  anchor: ChangeLogAnchor,
): void {
  db.prepare(SEED_CHANGE_LOG_STREAM_SQL).run({
    watermark: anchor.resumeWatermark,
    writeTimeMs: anchor.nowMs,
  });
}

/** Reads the whole meta row. Throws if the log has not been reconciled. */
export function readChangeLogMeta(db: Database): ChangeLogMeta {
  const meta = db
    .prepare(/*sql*/ `
      SELECT "epoch", "generation", "replicaID", "schemaVersion",
             "seededAtMs", "seedWatermark"
        FROM "${CHANGE_LOG_META_TABLE}"
    `)
    .get<ChangeLogMeta | undefined>();
  if (meta === undefined) {
    throw new Error('the SQLite change log has no meta row');
  }
  return meta;
}

/**
 * `max()` on the primary key's leading column, i.e. an index seek. Null when
 * the log is empty.
 */
export function readChangeLogHead(db: Database): string | null {
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
