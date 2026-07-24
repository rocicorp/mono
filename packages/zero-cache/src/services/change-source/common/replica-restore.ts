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

/** @returns `true` if the replica was restored, `false` if not found */
export async function restoreReplica(
  lc: LogContext,
  config: LitestreamConfig,
  replicaFile: string,
  replicaConstraints: ReplicaConstraints,
): Promise<boolean> {
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
    return attempt.restored;
  } finally {
    const attrs = litestreamRestoreMetricAttrs(config, 'replication_manager');
    const labels = {...attrs, result: result ?? 'error'};
    litestreamRestoreRuns().add(1, labels);
    litestreamRestoreDuration().recordMs(performance.now() - start, labels);
  }
}
