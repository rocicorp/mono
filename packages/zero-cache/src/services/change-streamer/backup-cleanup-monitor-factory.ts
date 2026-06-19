import type {LogContext} from '@rocicorp/logger';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import {getLastBackupTime} from '../litestream/commands.ts';
import type {BackupMonitor} from './backup-monitor.ts';
import type {ChangeStreamerService} from './change-streamer.ts';
import {
  Litestream3BackupMonitor,
  type BackupStateVerifier,
} from './litestream3-backup-monitor.ts';

export type BackupCleanupMonitorFactoryOptions = {
  lc: LogContext;
  config: NormalizedZeroConfig;
  replicaFile: string;
  changeStreamer: ChangeStreamerService;
  initialCleanupDelayMs: number;
  verifyBackupState?: BackupStateVerifier | undefined;
};

export function createBackupCleanupMonitor({
  lc,
  config,
  replicaFile,
  changeStreamer,
  initialCleanupDelayMs,
  verifyBackupState,
}: BackupCleanupMonitorFactoryOptions): BackupMonitor | null {
  const {backupURL, port: metricsPort} = config.litestream;
  if (!backupURL) {
    return null;
  }

  return new Litestream3BackupMonitor(
    lc,
    replicaFile,
    backupURL,
    `http://localhost:${metricsPort}/metrics`,
    changeStreamer,
    initialCleanupDelayMs,
    verifyBackupState ?? (() => getLastBackupTime(lc, config)),
  );
}
