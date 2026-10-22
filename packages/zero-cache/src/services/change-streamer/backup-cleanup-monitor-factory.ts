import type {LogContext} from '@rocicorp/logger';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import {
  backupDestinationURL,
  getLastBackupTime,
} from '../litestream/commands.ts';
import {BackupMonitor, type BackupPoller} from './backup-monitor.ts';
import {LitestreamSyncRequester} from './litestream-sync-requester.ts';
import {
  type BackupStateVerifier,
  Litestream3PrometheusPoller,
} from './litestream3-prometheus-poller.ts';
import {ReplicaPoller} from './replica-poller.ts';

export type BackupCleanupMonitorFactoryOptions = {
  lc: LogContext;
  config: NormalizedZeroConfig;
  replicaFile: string;
  verifyBackupState?: BackupStateVerifier | undefined;
};

export function createBackupCleanupMonitor({
  lc,
  config,
  replicaFile,
  verifyBackupState,
}: BackupCleanupMonitorFactoryOptions): BackupMonitor {
  const {litestream, replica} = config;
  const backupURL = backupDestinationURL(litestream);

  let poller: BackupPoller;

  if (!backupURL) {
    poller = new ReplicaPoller(lc, replicaFile);
  } else if (config.litestream.backupUsingV5) {
    poller = new LitestreamSyncRequester(lc, replicaFile, {
      intervalMs: litestream.incrementalBackupIntervalSeconds * 1000,
    });
  } else {
    const {port: metricsPort} = litestream;
    poller = new Litestream3PrometheusPoller(
      lc,
      replicaFile,
      backupURL,
      `http://localhost:${metricsPort}/metrics`,
      verifyBackupState ??
        (() => getLastBackupTime(lc, litestream, replica.file)),
    );
  }

  return new BackupMonitor(lc, poller, replicaFile);
}
