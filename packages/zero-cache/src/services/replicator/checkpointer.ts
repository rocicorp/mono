import {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import {randInt} from 'shared/src/rand.js';
import {promiseVoid} from 'shared/src/resolved-promises.js';
import {orTimeout} from 'zero-cache/src/types/timeout.js';
import {Database} from 'zqlite/src/db.js';
import {Notifier} from './notifier.js';

/**
 * A `Checkpointer` is consulted by the Replicator after each commit and given
 * the opportunity to perform a blocking WAL checkpoint before the Replicator
 * continues processing the replication stream.
 */
export interface Checkpointer {
  maybeCheckpoint(
    numCommittedChanges: number,
    notifier: Notifier,
  ): Promise<void>;

  stop(): void;
}

/**
 * The `NULL_CHECKPOINTER` is suitable for an environment in which checkpoints are
 * handled by an external entity, such as a `litestream replicate` process.
 */
export const NULL_CHECKPOINTER: Checkpointer = {
  maybeCheckpoint: () => promiseVoid,
  stop: () => {},
} as const;

export type CheckpointerConfig = {
  /**
   * The number of outstanding frames or changes at which a blocking
   * checkpoint is triggered.
   *
   * Defaults to 200.
   */
  threshold?: number;

  /**
   * The base timeout to block on an active checkpoint. This timeout is increased
   * in proportion to the size of WAL divided by the `threshold`. For example, if
   * the size of the WAL is twice the `threshold`, the timeout will be doubled.
   *
   * This dynamic timeout algorithm reduces the amount of system-wide pausing in
   * the common case, even in the presence of occasional, long-held locks due to
   * events like a long hydration. If checkpoint is not able to start within the
   * timeout, the checkpointer gives up and allows the system to progress. As the
   * size of the WAL grows, the checkpointer will wait for longer timeouts to ensure
   * that the checkpointing eventually succeeds.
   *
   * Defaults to 200 milliseconds.
   */
  baseCheckpointTimeoutMs?: number;

  /**
   * A regular interval at which passive checkpoints are attempted. This allows an
   * idle task (i.e. no sync connections) to clean up outside of the
   * replication path.
   *
   * Defaults to one minute.
   */
  passiveCheckpointPeriodMs?: number;
};

/**
 * The `WALCheckpointer` executes checkpoints when the number of WAL log entries
 * exceeds a configurable threshold. It does this by:
 *
 * 1. Broadcasting a `maintenance` ReplicaState message to signal view-syncers to
 *    release their locks.
 * 2. Executing `wal_checkpoint(TRUNCATE)` with a busy_timeout proportional to
 *    the outstanding log size.
 * 3. Broadcasting a `version-ready` ReplicaState message to signal view-syncers to
 *    reestablish their locks.
 *
 * Points of interest:
 *
 * * The checkpointer waits for the `maintenance` broadcast to be sent to over
 *   IPC channels (in-process blocking), but does not request or wait for ACKs from
 *   the view syncers. This is because the `wal_checkpoint(TRUNCATE)` command itself
 *   will block until all read locks are released, up to the configured `busy_timeout`.
 *
 * * After performing the checkpoint, however, the checkpointer _does_ request ACKs
 *   from view-syncers to confirm that they have reestablished their snapshots. There
 *   is otherwise no way to know when it is safe to continue replication. This is
 *   expected to be very fast, but to avoid pathological cases the wait is capped at
 *   100ms.
 */
export class WALCheckpointer implements Checkpointer {
  readonly #lc: LogContext;
  readonly #db: Database;
  readonly #threshold: number;
  readonly #baseCheckpointTimeoutMs: number;
  readonly #passiveCheckpointTimer: ReturnType<typeof setTimeout>;

  #logSize = 0;
  #outstandingChanges = 0;

  constructor(
    lc: LogContext,
    replicaDbFile: string,
    cfg: CheckpointerConfig = {},
  ) {
    const {
      threshold = 200,
      baseCheckpointTimeoutMs = 10,
      passiveCheckpointPeriodMs = 60_000,
    } = cfg;

    assert(
      threshold > 0 &&
        baseCheckpointTimeoutMs > 0 &&
        passiveCheckpointPeriodMs > 0,
      `Invalid config ${JSON.stringify(cfg)}`,
    );

    const db = new Database(lc, replicaDbFile);
    db.pragma('journal_mode = WAL');

    this.#lc = lc.withContext('component', 'wal-checkpointer');
    this.#db = db;
    this.#threshold = threshold;
    this.#baseCheckpointTimeoutMs = baseCheckpointTimeoutMs;
    this.#passiveCheckpointTimer = setInterval(
      () => this.#checkpoint('PASSIVE'),
      passiveCheckpointPeriodMs,
    );
  }

  stop() {
    clearTimeout(this.#passiveCheckpointTimer);
    this.#lc.info?.('stopped');
  }

  async maybeCheckpoint(changes: number, notifier: Notifier) {
    this.#outstandingChanges += changes;

    // Simplification: changes and frames equivalently w.r.t. the threshold.
    if (Math.max(this.#outstandingChanges, this.#logSize) >= this.#threshold) {
      // If no read locks are held, a PASSIVE checkpoint may suffice.
      // Regardless of success, #logSize gets updated to determine if a
      // blocking checkpoint is warranted.
      this.#checkpoint('PASSIVE');
    }

    if (this.#logSize >= this.#threshold) {
      const logSize = this.#logSize;

      const t0 = Date.now();
      await this.#enterMaintenanceMode(notifier);
      const t1 = Date.now();

      // The timeout is proportional to the size of the log compared to the threshold.
      const timeout =
        (this.#logSize / this.#threshold) * this.#baseCheckpointTimeoutMs;
      const result = this.#checkpoint('TRUNCATE', timeout);

      const t2 = Date.now();
      await this.#exitMaintenanceMode(notifier);
      const t3 = Date.now();

      this.#lc.info?.(
        `WAL(busy=${timeout}ms): pre=${t1 - t0}ms checkpoint=${
          t2 - t1
        }ms post=${t3 - t2}ms logSize=${logSize} result`,
        result,
      );
    }
  }

  async #enterMaintenanceMode(notifier: Notifier) {
    await Promise.all(notifier.notifySubscribers({state: 'maintenance'}));
  }

  async #exitMaintenanceMode(notifier: Notifier) {
    // Request an ACK when exiting maintenance mode to maximize the chance of
    // view syncers re-establishing snapshots at the same version. This is expected
    // to be very fast (single-digit milliseconds).
    //
    // However, this is also the chance for view syncers to invoke an occasional
    // `PRAGMA optimize` call, for which latency may be variable. To avoid holding
    // up the replication stream indefinitely, we cap the wait at 100ms.
    const result = await orTimeout(
      Promise.all(
        notifier.notifySubscribers({
          state: 'version-ready',
          ack: randomACK(),
        }),
      ),
      100,
    );
    if (result === 'timed-out') {
      this.#lc.info?.('timed out waiting for view-syncer resumption');
    }
  }

  #checkpoint(mode: CheckpointMode, timeoutMs?: number): CheckpointResult {
    if (timeoutMs) {
      this.#db.pragma(`busy_timeout = ${timeoutMs}`);
    }
    const result = checkpoint(this.#db, mode);

    this.#logSize = result.log;
    this.#outstandingChanges = 0;
    return result;
  }
}

type CheckpointMode = 'PASSIVE' | 'FULL' | 'RESTART' | 'TRUNCATE';

type CheckpointResult = {
  busy: number;
  log: number;
  checkpointed: number;
};

function checkpoint(db: Database, mode: CheckpointMode): CheckpointResult {
  const result = db.pragma(`wal_checkpoint(${mode})`) as CheckpointResult[];
  return result[0];
}

function randomACK() {
  return randInt(Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER);
}
