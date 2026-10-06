import type {LogContext} from '@rocicorp/logger';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import type {Source} from '../../types/streams.ts';
import {
  backupDestinationURL,
  getLastBackupTime,
} from '../litestream/commands.ts';
import {type BackedUpWatermark, BackupMonitor} from './backup-monitor.ts';
import type {ChangeStreamerService} from './change-streamer.ts';
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
  changeStreamer: ChangeStreamerService;
  verifyBackupState?: BackupStateVerifier | undefined;
};

export function createBackupCleanupMonitor({
  lc,
  config,
  replicaFile,
  changeStreamer,
  verifyBackupState,
}: BackupCleanupMonitorFactoryOptions): BackupMonitor {
  const {litestream, replica} = config;
  const backupURL = backupDestinationURL(litestream);

  let stream: Source<BackedUpWatermark>;

  if (!backupURL) {
    stream = new ReplicaPoller(lc, replicaFile).start();
  } else if (config.litestream.backupUsingV5) {
    stream = new LitestreamSyncRequester(lc, replicaFile, {
      intervalMs: litestream.incrementalBackupIntervalSeconds * 1000,
    }).start();
  } else {
    const {port: metricsPort} = litestream;
    stream = new Litestream3PrometheusPoller(
      lc,
      replicaFile,
      backupURL,
      `http://localhost:${metricsPort}/metrics`,
      verifyBackupState ??
        (() => getLastBackupTime(lc, litestream, replica.file)),
    ).start();
  }

  return new BackupMonitor(lc, stream, changeStreamer, replicaFile);
}
