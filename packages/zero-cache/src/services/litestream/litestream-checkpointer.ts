import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import type {ForceCheckpointConfig} from '../replicator/write-worker-client.ts';
import {RunningState} from '../running-state.ts';
import type {
  LitestreamController,
  SyncResponse,
} from './litestream-controller.ts';

const LITESTREAM_SYNC_TIMEOUT_MS = 10_000;
const PAUSE_POLL_INTERVAL_MS = 1_000;
const PAUSE_LOG_INTERVAL_POLLS = 30;

/**
 * Forces litestream to checkpoint the WAL from the write path, so a sustained
 * high write rate can't grow the WAL until litestream's own blocking TRUNCATE
 * checkpoint locks the db (see ReplicationPauseCheckpointConfig).
 *
 * Two tiers:
 *  - Soft (`checkpointThresholdPages`): after each committed transaction,
 *    once the un-checkpointed WAL backlog reaches the threshold, force an
 *    immediate litestream sync (which seals the WAL to LTX and attempts
 *    litestream's own PASSIVE checkpoint). Non-blocking beyond the sync call
 *    itself. Backs off by a full threshold's worth of WAL growth whenever an
 *    attempt fails to drain the WAL (e.g. litestream is mid-snapshot and its
 *    checkpoints are being skipped), so repeated failures don't reattempt on
 *    every commit.
 *  - Hard (`walMaxPages`, optional): if the WAL still exceeds this cap after
 *    a soft attempt, pause (block the caller) polling litestream until the
 *    WAL drains below it. This is the fail-safe against unbounded WAL growth
 *    when litestream structurally cannot checkpoint — e.g. an in-progress
 *    snapshot holds litestream's checkpoint lock for its full duration, so
 *    neither PASSIVE nor TRUNCATE checkpoints can run until it completes.
 */
export class LitestreamCheckpointer {
  readonly #lc: LogContext;
  readonly #db: Database;
  readonly #litestream: LitestreamController;
  readonly #attemptChunk: number;
  readonly #maxWalPages: number | undefined;
  readonly #state = new RunningState('litestream-checkpointer');
  #nextWalThreshold: number;

  constructor(
    lc: LogContext,
    db: Database,
    litestream: LitestreamController,
    config: ForceCheckpointConfig,
  ) {
    this.#lc = lc.withContext('litestream-checkpointer');
    this.#db = db;
    this.#litestream = litestream;
    this.#attemptChunk = config.checkpointThresholdPages;
    this.#nextWalThreshold = config.checkpointThresholdPages;
    this.#maxWalPages = config.maxWalPages;
  }

  #getNumWalPages(): number {
    // wal_checkpoint(NOOP) reads WAL status without doing any work: `log` is
    // the current WAL frame count and `checkpointed` is how many of those
    // frames are already applied to the main db, so `log - checkpointed` is
    // the un-checkpointed backlog that a forced sync would actually drain.
    // (Both reset once litestream restarts the WAL.)
    const [{log, checkpointed}] = this.#db.pragma<{
      log: number;
      checkpointed: number;
    }>('wal_checkpoint(NOOP)');
    return log - checkpointed;
  }

  /**
   * Call after a committed transaction. No-ops unless the WAL has grown to
   * at least the current threshold, in which case it triggers a litestream
   * sync to perform a checkpoint. Assuming that snapshots are disabled, the
   * checkpoint always succeeds. In the case where the server is configured to
   * perform snapshots and checkpoints fail, the "emergency break"
   * {@link #pauseUntilWalDrained()} option provides the disk use protection
   * that litestream's (disabled) truncate-page-n would have served.
   */
  async maybeCheckpoint(): Promise<void> {
    const walPages = this.#getNumWalPages();
    if (walPages < this.#nextWalThreshold) {
      return;
    }

    const start = performance.now();
    let result: SyncResponse | undefined;
    let err: unknown;
    try {
      result = await this.#litestream.sync(
        {
          wait: false,
          timeoutMs: LITESTREAM_SYNC_TIMEOUT_MS,
        },
        this.#state.signal,
      );
    } catch (e) {
      err = e;
    }

    const elapsed = performance.now() - start;
    let newWalPages = this.#getNumWalPages();
    if (newWalPages < this.#nextWalThreshold) {
      this.#lc.info?.(
        `checkpointed ${walPages} -> ${newWalPages} wal pages (${elapsed.toFixed(2)} ms)`,
        result,
      );
    } else if (result) {
      this.#lc.warn?.(
        `checkpoint skipped ${walPages} -> ${newWalPages}. a snapshot may be in progress (${elapsed.toFixed(2)} ms)`,
        result,
      );
    } else {
      this.#lc.warn?.(
        `checkpoint failed ${walPages} -> ${newWalPages} (${elapsed.toFixed(2)} ms)`,
        err,
      );
    }

    if (this.#maxWalPages !== undefined && newWalPages > this.#maxWalPages) {
      // Emergency brake when best-effort checkpoints are being skipped
      // (i.e. during a snapshot) and the WAL is approaching disk capacity.
      // Pausing replication is favorable to litestream's truncate-page-n
      // because the latter involves re-encoding the database under the lock
      // (https://github.com/benbjohnson/litestream/issues/1332), which would
      // incur the full latency of an additional snapshot.
      newWalPages = await this.#pauseUntilWalDrained(this.#maxWalPages);
    }

    // In the common case where checkpoints are successful, this is always
    // just `attemptChunk`. When a checkpoint doesn't drain the WAL (e.g.
    // litestream is performing a snapshot), wait for another chunk of growth
    // before the next attempt.
    this.#nextWalThreshold = newWalPages + this.#attemptChunk;
  }

  async #pauseUntilWalDrained(maxWalPages: number): Promise<number> {
    for (let i = 0; this.#state.shouldRun(); i++) {
      const walPages = this.#getNumWalPages();
      if (walPages < maxWalPages) {
        this.#lc.info?.(
          `wal size (${walPages} pages) is under the max limit of ${maxWalPages}`,
        );
        return walPages;
      }

      // Log every 30 seconds.
      if (i % PAUSE_LOG_INTERVAL_POLLS === 0) {
        this.#lc.warn?.(
          `waiting for litestream to successfully checkpoint ${walPages} wal pages`,
        );
      }
      await this.#state.sleep(PAUSE_POLL_INTERVAL_MS);
      await this.#litestream
        .sync(
          {wait: false, timeoutMs: LITESTREAM_SYNC_TIMEOUT_MS},
          this.#state.signal,
        )
        .catch(e => this.#lc.warn?.(`litestream /sync failed`, e));
    }
    throw new AbortError('litestream-controller stopped');
  }

  /** Releases the underlying litestream controller's connection. */
  close(): void {
    this.#litestream.close();
    this.#state.stop(this.#lc);
  }
}
