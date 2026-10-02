import type {LogContext} from '@rocicorp/logger';
import {sleep} from '../../../../../shared/src/sleep.ts';
import type {LitestreamConfig} from '../../../config/normalize.ts';
import {
  tryRestore,
  type ReplicaConstraints,
  type RestoreResult,
} from '../../litestream/commands.ts';
import {
  litestreamRestoreDuration,
  litestreamRestoreMetricAttrs,
  litestreamRestoreRuns,
} from '../../litestream/metrics.ts';
import type {SubscriptionState} from '../../replicator/schema/replication-state.ts';
import type {ChangeSource} from '../change-source.ts';
import type {InitCleanup} from './init-cleanup.ts';

export interface PurgeLock {
  release(): Promise<void>;
}

/** A PurgeLock that also constrains the replica that can be restored. */
export interface ConstrainingPurgeLock extends PurgeLock, ReplicaConstraints {}

/**
 * Acquires a purge lock on the PG change-log (if it is not empty).
 *
 * When resuming the change-log from a replication slot, `slotWatermark` is the
 * slot's position (as a watermark). If the change-log's head is behind it, the
 * change-log may be missing transactions that the slot will not stream, in
 * which case nothing is locked and `'behind-slot'` is returned.
 */
export type PgChangeLogPurgeLocker = (
  slotWatermark?: string,
) => Promise<ConstrainingPurgeLock | null | 'behind-slot'>;

export type RestoreOptions = {
  litestream?: LitestreamConfig;
  /**
   * With the PG change-log enabled, purge-locks the change-log before the
   * replica is restored, which constrains the replica to one from which the
   * change-log can be resumed. The lock is acquired as late as possible (i.e.
   * after a replication slot is created, if applicable) because, if the
   * change-db is the upstream db, a transaction holding the lock would block
   * the creation of a replication slot.
   */
  acquirePurgeLock?: PgChangeLogPurgeLocker | undefined;
  /**
   * Resources acquired by the initialization (e.g. a claimed replication slot)
   * are registered here to be released if the initialization attempt fails.
   * Note that a purge lock acquired with `acquirePurgeLock` must be registered
   * here by the locker, so that it is released on failure.
   */
  cleanup?: InitCleanup | undefined;
};

export type InitializeResult = {
  subscriptionState: SubscriptionState;
  changeSource: ChangeSource;
  destinationBackupURL: string | undefined;
  /**
   * The replica this change stream belongs to, from the upstream `replicas`
   * table. It is part of the identity the SQLite change log records, because a
   * generation (i.e. `replicaVersion`) is shared by every sibling of a forked
   * replica and so cannot distinguish two siblings' logs. `null` when the
   * change source has no upstream table to identify itself from.
   */
  replicaID: string | null;

  /**
   * Whether server readiness should be gated on the first litestream backup,
   * in order to ensure that a newly connecting subscriber can restore from
   * this change-streamer's backup.
   *
   * This should be true when the backup destination differs from where the
   * backup was restored from, which is the case for (1) initial-sync and
   * (2) most initialization cases in RMv2, with the exception being when
   * an abandoned replica is resumed.
   */
  waitForBackupBeforeServing: boolean;

  /**
   * Whether the PG change-log's head was behind the position of the
   * replication slot from which replication resumes (see
   * {@link PgChangeLogPurgeLocker}), in which case it is re-initialized from
   * the restored replica if it does not contain the replica's changes.
   */
  pgChangeLogBehindSlot?: boolean | undefined;

  /**
   * Whether the litestream backup to `destinationBackupURL` starts a new
   * (litestream v5) backup lineage, i.e. one that no local litestream state
   * belongs to. Any such state (e.g. from a previous run of a restarted
   * container, whose restore reused the existing replica) describes a
   * different lineage and must be discarded before replicating, or litestream
   * resolves its position from it and fails (and auto-recovers) on its first
   * sync to the empty lineage.
   */
  newBackupLineage: boolean;
};

// A short retry is much cheaper than an initial Postgres sync, while keeping a
// persistent failure from delaying startup appreciably.
const MAX_RESTORE_ATTEMPTS = 3;
const RESTORE_RETRY_DELAY_MS = 5_000;

export async function restoreReplica(
  lc: LogContext,
  config: LitestreamConfig,
  replicaFile: string,
  replicaConstraints: ReplicaConstraints | undefined,
): Promise<void> {
  const start = performance.now();
  let result: RestoreResult | undefined;
  try {
    // `tryRestore` returns ordinary restore outcomes (no backup or an invalid
    // replica) rather than throwing; preserve their existing immediate
    // initial-sync fallback. Retry every thrown restore error twice before
    // falling back to the initial Postgres sync.
    for (let attemptNum = 1; attemptNum <= MAX_RESTORE_ATTEMPTS; attemptNum++) {
      try {
        const attempt = await tryRestore(
          lc,
          config,
          replicaFile,
          replicaConstraints,
          'replication_manager',
        );
        result = attempt.result;
        return;
      } catch (e) {
        if (attemptNum === MAX_RESTORE_ATTEMPTS) {
          lc.error?.(
            `litestream restore failed after ${attemptNum} attempts; resyncing the replica`,
            e,
          );
          return;
        }

        lc.warn?.(
          `litestream restore attempt ${attemptNum} failed; retrying in ${RESTORE_RETRY_DELAY_MS}ms (attempt ${attemptNum + 1} of ${MAX_RESTORE_ATTEMPTS})`,
          e,
        );
        await sleep(RESTORE_RETRY_DELAY_MS);
      }
    }
  } finally {
    const attrs = litestreamRestoreMetricAttrs(config, 'replication_manager');
    const labels = {...attrs, result: result ?? 'error'};
    litestreamRestoreRuns().add(1, labels);
    litestreamRestoreDuration().recordMs(performance.now() - start, labels);
  }
}
