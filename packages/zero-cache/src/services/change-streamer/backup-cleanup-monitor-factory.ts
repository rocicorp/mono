import type {LogContext} from '@rocicorp/logger';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import {BACKUP_WATERMARK_READER_URL} from '../../server/worker-urls.ts';
import {forkChildWorker} from '../../types/processes.ts';
import {getLastBackupTime} from '../litestream/commands.ts';
import {VfsBackupWatermarkWorkerSource} from '../litestream/vfs-watermark-worker-source.ts';
import type {BackupMonitor} from './backup-monitor.ts';
import type {ChangeStreamerService} from './change-streamer.ts';
import {
  Litestream3BackupMonitor,
  type BackupStateVerifier,
} from './litestream3-backup-monitor.ts';
import {
  VfsBackupMonitor,
  type VfsBackupWatermarkSource,
} from './vfs-backup-monitor.ts';

export type BackupCleanupMonitorFactoryOptions = {
  lc: LogContext;
  config: NormalizedZeroConfig;
  replicaFile: string;
  changeStreamer: ChangeStreamerService;
  initialCleanupDelayMs: number;
  verifyBackupState?: BackupStateVerifier | undefined;
  vfsBackupWatermarkSource?: VfsBackupWatermarkSource | undefined;
  env?: NodeJS.ProcessEnv | undefined;
};

export function createBackupCleanupMonitor({
  lc,
  config,
  replicaFile,
  changeStreamer,
  initialCleanupDelayMs,
  verifyBackupState,
  vfsBackupWatermarkSource,
  env,
}: BackupCleanupMonitorFactoryOptions): BackupMonitor | null {
  const {litestream, replica} = config;
  const {backupURL, port: metricsPort} = litestream;
  if (!backupURL) {
    return null;
  }

  if (config.litestream.backupUsingV5) {
    return new VfsBackupMonitor(
      lc,
      backupURL,
      changeStreamer,
      initialCleanupDelayMs,
      config.litestream.vfsProbeIntervalMs,
      vfsBackupWatermarkSource ??
        new VfsBackupWatermarkWorkerSource(
          lc,
          () =>
            forkChildWorker(BACKUP_WATERMARK_READER_URL, env ?? process.env),
          config.litestream.vfsProbeTimeoutMs,
        ),
    );
  }

  return new Litestream3BackupMonitor(
    lc,
    replicaFile,
    backupURL,
    `http://localhost:${metricsPort}/metrics`,
    changeStreamer,
    initialCleanupDelayMs,
    verifyBackupState ??
      (() => getLastBackupTime(lc, litestream, replica.file)),
  );
}
