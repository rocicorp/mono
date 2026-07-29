import type {LogContext} from '@rocicorp/logger';
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
};

export async function restoreReplica(
  lc: LogContext,
  config: LitestreamConfig,
  replicaFile: string,
  replicaConstraints: ReplicaConstraints | undefined,
): Promise<void> {
  const start = performance.now();
  let result: RestoreResult | undefined;
  try {
    const attempt = await tryRestore(
      lc,
      config,
      replicaFile,
      replicaConstraints,
      'replication_manager',
    );
    result = attempt.result;
  } catch (e) {
    lc.error?.(
      `error restoring backup. resyncing the replica: ${String(e)}`,
      e,
    );
  } finally {
    const attrs = litestreamRestoreMetricAttrs(config, 'replication_manager');
    const labels = {...attrs, result: result ?? 'error'};
    litestreamRestoreRuns().add(1, labels);
    litestreamRestoreDuration().recordMs(performance.now() - start, labels);
  }
}
