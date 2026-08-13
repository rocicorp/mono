import type {LogContext} from '@rocicorp/logger';
import {SqliteError} from '@rocicorp/zero-sqlite3';
import {must} from '../../../../shared/src/must.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {
  isSQLiteCorruption,
  logSQLiteCorruptionDiagnostics,
} from '../../db/sqlite-corruption.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  readCookieRowCounts,
  readCookies,
} from '../replicator/change-log-cookies.ts';
import {
  ChangeLogRebuildRequired,
  changeLogFileName,
  changeLogWipeReason,
  deleteChangeLogDB,
  openChangeLogDBForWriting,
  readChangeLogHead,
  rebuildChangeLogDBForWriting,
  reconcileChangeLog,
  type ChangeLogAnchor,
  type ChangeLogIdentity,
  type ChangeLogResumePoint,
  type ReconcileResult,
} from '../replicator/change-log-db.ts';
import {ChangeLogStreamWriter} from '../replicator/change-log-stream-writer.ts';
import {
  getSQLiteChangeLogInfo,
  logSQLiteChangeLogStartup,
  recordSQLiteChangeLogReconcile,
  SQLiteChangeLogObserver,
  type ChangeLogCommit,
  type SQLiteChangeLogObservabilityState,
} from '../replicator/sqlite-change-log-observability.ts';

export type SQLiteChangeLogWriterOptions = {
  /** The replica file the log is named after, i.e. `${replicaFile}-change-log`. */
  replicaFile: string;
  /** The replica whose contents this log belongs to. */
  identity: ChangeLogIdentity;
  /**
   * Called after each transaction's log commit, with the log's new head. This
   * is what releases the catchup barrier: after the writer moved in-process,
   * the head advances here rather than at a subscriber's ACK.
   */
  onCommit?: ((watermark: string) => void) | undefined;
  /**
   * Called when the writer disables itself and deletes the file. The reader is
   * cached on the service, so a delete that leaves it open would leave an
   * in-process reader serving an unlinked inode while every new open sees
   * nothing.
   */
  onDisabled?: (() => void) | undefined;
  /** Called when reconciliation replaces the file, to close cached readers. */
  onRebuilt?: (() => void) | undefined;
  /** Overridable for tests. */
  now?: (() => number) | undefined;
};

/**
 * Appends the canonical change stream to the SQLite change log, from the
 * change-streamer's own stream loop.
 *
 * ### Ordering
 *
 * The log's commit for a transaction precedes the forward of that transaction's
 * `commit` message, and nothing `await`s in between. Everything that can
 * advance the watermark the stream would resume from — the storer's Postgres
 * commit, `lastWatermark`, the upstream ACK — runs strictly after it, so the
 * log's head is always at or above the resume point. A *hole* is therefore
 * unreachable at any crash timing, and a *phantom* is the normal state, which is
 * what makes truncate-above the whole of reconciliation.
 *
 * ### Fail-soft
 *
 * The log is a catchup cache: nothing in it is not either already durable in the
 * replica's litestream backup or re-derivable from the replication slot. A write
 * failure must therefore not stop replication for the shard. Corruption is
 * rebuilt once at open; any other write error — a full disk, an I/O error, a
 * permissions problem — logs, disables the writer, deletes the file, closes the
 * reader, and lets the stream continue. The cost is catchup reach, and reach is
 * recoverable: `too-old` sends a subscriber to a restore.
 *
 * One carve-out keeps that from hiding the bug class this topology introduces. A
 * missed truncate-above surfaces as `SQLITE_CONSTRAINT` on the writer's plain
 * `INSERT`, which is "any write error other than corruption" and would be
 * absorbed quietly. It is counted as an invariant failure and alerted on before
 * failing soft.
 */
export class SQLiteChangeLogWriter {
  readonly #lc: LogContext;
  readonly #opts: SQLiteChangeLogWriterOptions;
  readonly #now: () => number;

  #db: Database | undefined;
  #writer: ChangeLogStreamWriter | undefined;
  #observer: SQLiteChangeLogObserver | undefined;
  #disabled = false;

  constructor(lc: LogContext, opts: SQLiteChangeLogWriterOptions) {
    this.#lc = lc.withContext('component', 'sqlite-change-log-writer');
    this.#opts = opts;
    this.#now = opts.now ?? Date.now;
  }

  /** False once the writer has failed soft, or after {@link close}. */
  get enabled(): boolean {
    return !this.#disabled;
  }

  /**
   * The writer's own connection, which is the one purge runs on (§3.3): one
   * process appends *and* deletes, on one handle, with no second writable
   * connection and no cross-process barrier. `undefined` until the stream
   * loop's first reconcile creates the log, and again once the writer has
   * failed soft and deleted the file — both of which a purge cycle treats as
   * "skip", not as an error.
   */
  get connection(): Database | undefined {
    return this.#db;
  }

  /** Exposed for tests and for the integration assertions on the metrics. */
  state(): SQLiteChangeLogObservabilityState | undefined {
    return this.#observer?.state();
  }

  /**
   * Opens the log if necessary and reconciles it with the Postgres resume
   * point. Returns the resume point stored in the log.
   *
   * Returns `undefined` when the writer is disabled. The caller must then use
   * another source.
   *
   * This runs for every connection because Postgres supplies a new resume point
   * after each reconnect. The log can contain transactions above that point.
   * Reconciliation removes them before Postgres sends them again.
   *
   * `resumeFrom` includes cookies because the cookies must match the resume
   * watermark.
   */
  reconcile(
    resumeFrom: ChangeLogResumePoint,
  ): ChangeLogResumePoint | undefined {
    return this.#reconcile(() => resumeFrom);
  }

  /**
   * Reconciles the log without a Postgres resume point.
   *
   * A valid log uses its own head and cookies. A new or invalid log uses the
   * replica values returned by `seed`.
   */
  reconcileFromLog(
    seed: () => ChangeLogResumePoint,
  ): ChangeLogResumePoint | undefined {
    return this.#reconcile(db => this.#resumeFromLog(db, seed));
  }

  #reconcile(
    resolve: (db: Database) => ChangeLogResumePoint,
  ): ChangeLogResumePoint | undefined {
    if (this.#disabled) {
      return undefined;
    }
    const {replicaFile, identity} = this.#opts;
    const nowMs = this.#now();
    const anchor = (db: Database): ChangeLogAnchor => ({
      ...resolve(db),
      identity,
      nowMs,
    });
    try {
      const db = this.#db;
      if (db === undefined) {
        const opened = openChangeLogDBForWriting(this.#lc, replicaFile, anchor);
        this.#db = opened.db;
        if (opened.replacedFile) {
          this.#opts.onRebuilt?.();
        }
        recordSQLiteChangeLogReconcile(this.#lc, opened.result);
        const info = getSQLiteChangeLogInfo(opened.db);
        logSQLiteChangeLogStartup(
          this.#lc,
          changeLogFileName(replicaFile),
          info,
        );
        this.#observer = new SQLiteChangeLogObserver(this.#lc, info);
      } else {
        const resolved = anchor(db);
        let result: ReconcileResult;
        try {
          result = reconcileChangeLog(this.#lc, db, resolved, {
            rebuildInsteadOfUnboundedWork: true,
          });
        } catch (e) {
          if (!(e instanceof ChangeLogRebuildRequired)) {
            throw e;
          }
          // Close every handle to the old inode before unlinking it. The writer
          // is reconstructed below against the replacement connection.
          this.#writer = undefined;
          this.#db = undefined;
          db.close();
          this.#opts.onRebuilt?.();
          const rebuilt = rebuildChangeLogDBForWriting(
            this.#lc,
            replicaFile,
            resolved,
            e,
          );
          this.#db = rebuilt.db;
          result = rebuilt.result;
        }
        recordSQLiteChangeLogReconcile(this.#lc, result);
        if (result.action !== 'none') {
          this.#observer?.reconciled(
            getSQLiteChangeLogInfo(
              must(this.#db, 'the SQLite change log is not open'),
            ),
          );
        }
      }
      // A reseed drops and recreates the stream table, so the append statements
      // are prepared fresh against whatever the reconciliation left behind.
      const reconciledDB = must(this.#db, 'the SQLite change log is not open');
      this.#writer = new ChangeLogStreamWriter(
        new StatementRunner(reconciledDB),
      );
      // Read both values after reconciliation. A truncate or reseed updates the
      // head and cookies in the same transaction.
      return {
        resumeWatermark: must(
          readChangeLogHead(reconciledDB),
          'the SQLite change log has no head after reconciliation',
        ),
        cookies: readCookies(reconciledDB),
      };
    } catch (e) {
      this.#failSoft('reconciling the SQLite change log', e);
      return undefined;
    }
  }

  /**
   * Returns the resume point supplied by the log.
   *
   * A valid log uses its own head and cookies. A log that must be recreated uses
   * the replica values returned by `seed`.
   */
  #resumeFromLog(
    db: Database,
    seed: () => ChangeLogResumePoint,
  ): ChangeLogResumePoint {
    if (changeLogWipeReason(db, this.#opts.identity) !== undefined) {
      return seed();
    }
    const head = readChangeLogHead(db);
    // An empty log has no resume point, so use the replica seed.
    return head === null
      ? seed()
      : {resumeWatermark: head, cookies: readCookies(db)};
  }

  /**
   * Writes one stream message, committing at transaction boundaries. Never
   * throws: a write failure disables the writer instead of failing the stream.
   */
  write(change: ChangeStreamData, json: string): void {
    const writer = this.#writer;
    const db = this.#db;
    if (this.#disabled || writer === undefined || db === undefined) {
      return;
    }
    const observer = this.#observer;
    observer?.messageReceived(change, json);
    const [type, msg] = change;
    const startedAt = performance.now();
    try {
      let commit: ChangeLogCommit | null = null;
      switch (type) {
        case 'begin':
          writer.begin(change[2].commitWatermark, json);
          break;
        case 'commit': {
          const {watermark} = change[2];
          const commitStart = performance.now();
          const stats = writer.commit(watermark, json, this.#now());
          commit = {
            watermark,
            stats,
            logCommitMs: performance.now() - commitStart,
          };
          break;
        }
        case 'rollback':
          writer.rollback();
          break;
        default:
          writer.append(json, msg);
          break;
      }
      observer?.messageProcessed(change, commit, performance.now() - startedAt);
      if (commit !== null && commit.stats.cookieMutations.length > 0) {
        // Only re-counted when a schema change actually moved the cookie jar,
        // which keeps the count off the forward path of every other
        // transaction. The counts are read after the commit, so they are what
        // a reader of the file would see.
        observer?.cookiesMutated(
          commit.stats.cookieMutations,
          readCookieRowCounts(db),
        );
      }
      if (commit !== null) {
        // The head advanced, and it is durable. Releasing the barrier here is
        // what makes its poll interval a backstop rather than the mechanism.
        this.#opts.onCommit?.(commit.watermark);
      }
    } catch (e) {
      observer?.messageFailed(change, e, performance.now() - startedAt);
      if (isConstraintViolation(e)) {
        // Reconciliation should have truncated whatever this collided with.
        // Report it as an invariant failure before failing soft, so that a
        // truncate-above bug cannot look like a lost cache.
        observer?.invariantFailure(
          'SQLite change-log insert violated a constraint, which means a ' +
            'transaction above the resume watermark was not truncated',
          {tag: msg.tag, error: String(e)},
        );
      }
      this.#failSoft('writing to the SQLite change log', e);
    }
  }

  /**
   * Discards the open transaction, if any. Called when the change stream is
   * interrupted mid-transaction.
   */
  abort(): void {
    const writer = this.#writer;
    if (this.#disabled || writer === undefined) {
      return;
    }
    this.#observer?.abort();
    try {
      writer.rollback();
    } catch (e) {
      this.#failSoft('rolling back the SQLite change log', e);
    }
  }

  close(): void {
    this.#disabled = true;
    this.#writer = undefined;
    this.#observer = undefined;
    const db = this.#db;
    this.#db = undefined;
    try {
      db?.close();
    } catch (e) {
      this.#lc.warn?.('error closing the SQLite change log', e);
    }
  }

  /**
   * Logs, disables the writer, deletes the file, and closes the reader, leaving
   * replication running. Disabling is for the life of the process: a log that
   * failed a write is not re-opened until the task restarts, so the behavior a
   * subscriber sees is the same as `sqliteChangeLogMode=off`.
   */
  #failSoft(what: string, e: unknown): void {
    const file = changeLogFileName(this.#opts.replicaFile);
    if (isSQLiteCorruption(e)) {
      // Open-time corruption is rebuilt by openChangeLogDBForWriting. Reaching
      // here means the file went bad under an open handle, which the same
      // delete-and-reseed answers -- on the next process start rather than now.
      logSQLiteCorruptionDiagnostics(this.#lc, 'change-log', file, e);
    }
    this.#lc.error?.(
      `error ${what}: disabling the writer and deleting ${file}. ` +
        `Replication continues; catchup reach is lost until this task restarts.`,
      e,
    );
    this.close();
    try {
      deleteChangeLogDB(this.#opts.replicaFile);
    } catch (deleteError) {
      this.#lc.error?.(`unable to delete ${file}`, deleteError);
    }
    this.#opts.onDisabled?.();
  }
}

function isConstraintViolation(e: unknown): boolean {
  return e instanceof SqliteError && e.code.startsWith('SQLITE_CONSTRAINT');
}
