/**
 * Where a stream connection's initialization parameters come from.
 *
 * A change-streamer starts a stream from three things read at one position: the
 * watermark to resume at, the {@link BackfillRequest}s to hand the change
 * source, and the cookie set those requests were derived from. They come from
 * whichever change log is in use:
 *
 * - The Postgres change log supplies them itself
 *   (`Storer.getStartStreamInitializationParameters()`), after it is first
 *   re-initialized from the restored replica if it may be missing transactions
 *   that the replication slot will not stream.
 *
 * - The SQLite change log (i.e. when the Postgres change log is disabled)
 *   resumes from its own head when it is valid. A new or invalid log is seeded
 *   from the replica, which also supplies the resume point when the change-log
 *   writer is unavailable.
 */

import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {BackfillRequest} from '../change-source/protocol/current/upstream.ts';
import {
  backfillRequestsFrom,
  type CookieSet,
} from '../replicator/change-log-cookies.ts';
import type {ChangeLogResumePoint} from '../replicator/change-log-db.ts';
import {
  readBackfillRequests,
  readReplicaCookies,
} from '../replicator/schema/backfilling.ts';
import {getReplicationState} from '../replicator/schema/replication-state.ts';

/** Everything one stream connection starts from, read at one position. */
export type InitializationParameters = {
  readonly lastWatermark: string;
  readonly backfillRequests: BackfillRequest[];
  readonly cookies: CookieSet;
};

export type ChangeLogInitializerSources = {
  /**
   * `Storer.getStartStreamInitializationParameters()`. Only consulted when
   * initializing from the Postgres change log.
   */
  readonly pgChangeLog: () => Promise<InitializationParameters>;
  /** Typically {@link replicaInitializationSource}. */
  readonly replica: () => InitializationParameters;
  /**
   * Reconciles the SQLite change log and returns its current resume point: a
   * valid log uses its own head, and a new or invalid log uses `seed`. Only
   * consulted when not initializing from the Postgres change log.
   *
   * Returns `undefined` when the writer is disabled or unavailable.
   */
  readonly reconcileChangeLog: (
    seed: () => ChangeLogResumePoint,
  ) => ChangeLogResumePoint | undefined;
  /**
   * Re-initializes the Postgres change log from the replica's resume point
   * (taking ownership of it) if it does not contain all of the replica's
   * changes, re-checking under lock. Typically
   * `Storer.reinitializeFromReplica()`.
   *
   * Supplied only when the change log's head is behind the position of the
   * replication slot from which replication resumes, i.e. when it may be
   * missing transactions that the slot will not stream. Otherwise, a replica
   * that is ahead of the change log (as changes are forwarded before they are
   * stored) is caught up by the change stream, which resumes from the change
   * log's head.
   */
  readonly reinitializePgChangeLog?:
    | ((params: {
        watermark: string;
        cookies: CookieSet;
      }) => Promise<'reinitialized' | 'maintained'>)
    | undefined;
};

export class ChangeLogInitializer {
  readonly #lc: LogContext;
  readonly #initFromPgChangeLog: boolean;
  readonly #sources: ChangeLogInitializerSources;

  /**
   * The (restored) replica's resume point that the Postgres change log must
   * cover, read once. See {@link #pgChangeLog}.
   */
  #restoredReplica: InitializationParameters | undefined;

  /**
   * @param initFromPgChangeLog Whether the Postgres change log (rather than
   *        the SQLite change log) supplies the initialization parameters.
   */
  constructor(
    lc: LogContext,
    initFromPgChangeLog: boolean,
    sources: ChangeLogInitializerSources,
  ) {
    this.#lc = lc.withContext('component', 'change-log-initializer');
    this.#initFromPgChangeLog = initFromPgChangeLog;
    this.#sources = sources;
  }

  /**
   * Returns the initialization parameters for one stream connection, after
   * reconciling the SQLite change log when it is the one in use.
   */
  async initialize(): Promise<InitializationParameters> {
    return this.#initFromPgChangeLog
      ? await this.#pgChangeLog()
      : this.#sqliteChangeLog();
  }

  /**
   * The replica is read up front, as it seeds a new log and supplies the
   * resume point when the writer is unavailable.
   */
  #sqliteChangeLog(): InitializationParameters {
    const replica = this.#sources.replica();
    // Use the reconciled log position. If the writer is unavailable, use the
    // replica position.
    const resumePoint =
      this.#sources.reconcileChangeLog(() => resumePointOf(replica)) ??
      resumePointOf(replica);
    return {
      lastWatermark: resumePoint.resumeWatermark,
      cookies: resumePoint.cookies,
      // Derive the requests from these cookies so that the requests and the
      // watermark describe the same position.
      backfillRequests: backfillRequestsFrom(resumePoint.cookies),
    };
  }

  /**
   * Reads the Postgres change log's initialization parameters, first
   * re-initializing it from the replica (taking ownership of it) if it may be
   * missing transactions that the replication slot will not stream, e.g. if it
   * is no longer maintained after running in high-availability mode. This is
   * the Postgres counterpart of seeding a new SQLite change log from the
   * replica.
   *
   * This only applies when the change log's head is behind the slot's position
   * (i.e. when {@link ChangeLogInitializerSources.reinitializePgChangeLog} is
   * supplied), and the change log is only re-initialized if its head is also
   * behind the replica's. Otherwise, it contains every transaction before the
   * slot's position: one that was acked after being stored would be in the
   * change log, and one that was acked after being backed up would be in the
   * replica, which the change log's head covers.
   *
   * The replica is read only once, i.e. as it was restored from the backup,
   * before any stream is started. The live replica is no reference: it is
   * caught up from the forwarded changes, which can be ahead of what has been
   * stored, e.g. by the change log's previous owner. Against that fixed
   * resume point, re-initialization is idempotent: afterwards, the change log's
   * head (which only advances) covers it, so subsequent stream connections
   * leave it untouched.
   */
  async #pgChangeLog(): Promise<InitializationParameters> {
    const pg = await this.#sources.pgChangeLog();
    const reinitialize = this.#sources.reinitializePgChangeLog;
    if (!reinitialize) {
      return pg;
    }
    this.#restoredReplica ??= this.#sources.replica();
    const {lastWatermark, cookies} = this.#restoredReplica;
    if (pg.lastWatermark >= lastWatermark) {
      return pg;
    }
    const result = await reinitialize({watermark: lastWatermark, cookies});
    this.#lc.info?.(
      `Postgres change log at ${pg.lastWatermark} is behind the replica at ` +
        `${lastWatermark}: ${result}`,
    );
    // Either way, the change log has changed since it was read.
    return this.#sources.pgChangeLog();
  }
}

const resumePointOf = ({
  lastWatermark,
  cookies,
}: InitializationParameters): ChangeLogResumePoint => ({
  resumeWatermark: lastWatermark,
  cookies,
});

/**
 * Reads the replica's initialization parameters in one snapshot, which is
 * invariant 15 on the replica side: the state version and the cookies are only
 * meaningful together.
 */
export function readReplicaInitializationParameters(
  db: Database,
): InitializationParameters {
  const runner = new StatementRunner(db);
  runner.begin(); // deferred, i.e. one read snapshot for the three statements
  try {
    return {
      lastWatermark: getReplicationState(runner).stateVersion,
      backfillRequests: readBackfillRequests(db),
      cookies: readReplicaCookies(db),
    };
  } finally {
    if (db.inTransaction) {
      runner.rollback();
    }
  }
}

/**
 * The replica source for {@link ChangeLogInitializer}, opening and closing the
 * file per stream connection rather than holding a handle on a database another
 * process owns.
 *
 * The handle is read-only, which is invariant 19 stated in the API: the
 * replica's cookie tables are written by the change-processor and by nothing
 * else. Note that a read-only SQLite connection still needs to attach the
 * WAL's `-shm`, so this can fail if it opens at a moment when no writer holds
 * the replica.
 */
export function replicaInitializationSource(
  lc: LogContext,
  replicaFile: string,
): () => InitializationParameters {
  return () => {
    const db = new Database(
      lc.withContext('component', 'change-log-initializer'),
      replicaFile,
      {readonly: true},
    );
    try {
      return readReplicaInitializationParameters(db);
    } finally {
      db.close();
    }
  };
}
