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

export type RestoreOptions = {
  litestream?: LitestreamConfig;
  constraints?: ReplicaConstraints | undefined;
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
