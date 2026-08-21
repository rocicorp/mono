import type {LogContext} from '@rocicorp/logger';
import {must} from '../../../../shared/src/must.ts';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import type {Source} from '../../types/streams.ts';
import {getLastBackupTime} from '../litestream/commands.ts';
import {type BackedUpWatermark, BackupMonitor} from './backup-monitor.ts';
import type {ChangeStreamerService} from './change-streamer.ts';
import {
  type BackupStateVerifier,
  Litestream3PrometheusPoller,
} from './litestream3-prometheus-poller.ts';
import {ReplicaPoller} from './replica-poller.ts';
import {VfsWatermarkPoller} from './vfs-watermark-poller.ts';

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
  const {log, litestream, replica} = config;
  const {backupURL} = litestream;

  let stream: Source<BackedUpWatermark>;

  if (!backupURL) {
    stream = new ReplicaPoller(lc, replicaFile).start();
  } else if (config.litestream.backupUsingV5) {
    const {
      logLevel,
      endpoint,
      region,
      vfsQueryExecutable,
      vfsPollIntervalMs: remotePollIntervalMs,
    } = litestream;
    stream = new VfsWatermarkPoller(lc, replicaFile, {
      executable: must(
        vfsQueryExecutable,
        `litestream-vfs-query-executable must be defined`,
      ),
      remotePollIntervalMs,
      backupURL,
      region,
      endpoint,
      logLevel,
      logFormat: log.format,
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
