import type {LogContext} from '@rocicorp/logger';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import {BACKUP_WATERMARK_POLLER_URL} from '../../server/worker-urls.ts';
import {forkChildWorker} from '../../types/processes.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {ProcessManager} from '../life-cycle.ts';
import {getLastBackupTime} from '../litestream/commands.ts';
import {
  type BackupWatermarkUpdate,
  getVfsEnv,
} from '../litestream/vfs-watermark-poller.ts';
import {type BackedUpWatermark, BackupMonitor} from './backup-monitor.ts';
import type {ChangeStreamerService} from './change-streamer.ts';
import {
  type BackupStateVerifier,
  Litestream3PrometheusPoller,
} from './litestream3-prometheus-poller.ts';
import {ReplicaPoller} from './replica-poller.ts';

export type BackupCleanupMonitorFactoryOptions = {
  lc: LogContext;
  config: NormalizedZeroConfig;
  processes: ProcessManager;
  replicaFile: string;
  changeStreamer: ChangeStreamerService;
  verifyBackupState?: BackupStateVerifier | undefined;
  env?: NodeJS.ProcessEnv | undefined;
};

export function createBackupCleanupMonitor({
  lc,
  config,
  processes,
  replicaFile,
  changeStreamer,
  verifyBackupState,
  env = process.env,
}: BackupCleanupMonitorFactoryOptions): BackupMonitor {
  const {litestream, replica} = config;
  const {backupURL} = litestream;

  let stream: Source<BackedUpWatermark>;

  if (!backupURL) {
    stream = new ReplicaPoller(lc, replicaFile).start();
  } else if (config.litestream.backupUsingV5) {
    const {
      logLevel,
      endpoint,
      region,
      vfsLogFile: logFile,
      vfsProbeIntervalMs: remotePollIntervalMs,
    } = litestream;
    const sub = Subscription.create<BackedUpWatermark>();
    const vfsEnv = getVfsEnv({
      backupURL,
      endpoint,
      region,
      logLevel,
      logFile,
      remotePollIntervalMs,
    });
    processes
      .addWorker(
        forkChildWorker(BACKUP_WATERMARK_POLLER_URL, {...env, ...vfsEnv}),
        'supporting',
        'backup-watermark-poller',
      )
      .onMessageType<BackupWatermarkUpdate>('backupWatermarkUpdate', msg =>
        sub.push(msg),
      );
    stream = sub;
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
