import type {LogContext} from '@rocicorp/logger';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import {
  getOrCreateCounter,
  getOrCreateGauge,
} from '../../observability/metrics.ts';
import {
  litestreamBackupVerificationDuration,
  litestreamMonitorMetricAttrs,
} from '../litestream/metrics.ts';
import type {VfsBackupWatermark} from '../litestream/vfs-watermark-reader.ts';
import {RunningState} from '../running-state.ts';
import type {BackupMonitor} from './backup-monitor.ts';
import type {ChangeStreamerService} from './change-streamer.ts';

export interface VfsBackupWatermarkSource {
  readWatermark(): Promise<VfsBackupWatermark>;
  close?(): void;
}

/**
 * Monitors a Litestream v0.5.x backup by reading Zero's replication state from
 * the backup itself through the Litestream SQLite VFS.
 */
export class VfsBackupMonitor implements BackupMonitor {
  readonly id = 'vfs-backup-monitor';
  readonly #lc: LogContext;
  readonly #backupURL: string;
  readonly #changeStreamer: ChangeStreamerService;
  readonly #source: VfsBackupWatermarkSource;
  readonly #probeIntervalMs: number;
  readonly #state = new RunningState(this.id);

  readonly #watermarks = new Map<string, VfsBackupWatermark>();

  readonly #purgesBlocked = getOrCreateCounter('replica', 'purge_blocked', {
    description:
      'Number of change-log purges blocked because the actual backup ' +
      'watermark could not be read through the Litestream VFS.',
  });

  #lastWatermark: string = '';
  #latestBackupWatermark: VfsBackupWatermark | undefined;
  #checkWatermarkTimer: NodeJS.Timeout | undefined;

  constructor(
    lc: LogContext,
    backupURL: string,
    changeStreamer: ChangeStreamerService,
    probeIntervalMs: number,
    source: VfsBackupWatermarkSource,
  ) {
    this.#lc = lc.withContext('component', this.id);
    this.#backupURL = backupURL;
    this.#changeStreamer = changeStreamer;
    this.#source = source;
    this.#probeIntervalMs = probeIntervalMs;
  }

  run(): Promise<void> {
    this.#lc.info?.(`monitoring v5 backups at ${this.#backupURL} `);
    this.#checkWatermarkTimer = setInterval(
      this.checkWatermarkAndScheduleCleanup,
      this.#probeIntervalMs,
    );
    this.#initBackupLagMetric();
    return this.#state.stopped();
  }

  // Exported for testing
  readonly checkWatermarkAndScheduleCleanup = async () => {
    try {
      await this.#checkWatermark();
    } catch (e) {
      this.#purgesBlocked.add(1, {reason: 'vfs-probe-failed'});
      this.#lc.warn?.(
        `unable to read backup watermark through Litestream VFS. ` +
          `skipping change-log cleanup`,
        e,
      );
      return;
    }

    this.#scheduleCleanup();
  };

  async #checkWatermark(): Promise<void> {
    const watermark = await this.#readWatermarkTimed();
    this.#latestBackupWatermark = watermark;
    if (
      watermark.watermark > this.#lastWatermark &&
      !this.#watermarks.has(watermark.watermark)
    ) {
      this.#lc.info?.(
        `observed backup watermark=${watermark.watermark} through ` +
          `Litestream VFS at ${new Date(watermark.observedAtMs).toISOString()}`,
        {
          writeTimeMs: watermark.writeTimeMs,
          txid: watermark.txid,
          lagSeconds: watermark.lagSeconds,
        },
      );
      this.#watermarks.set(watermark.watermark, watermark);
    }
  }

  async #readWatermarkTimed(): Promise<VfsBackupWatermark> {
    const start = performance.now();
    let result: 'success' | 'error' = 'error';
    try {
      const ret = await this.#source.readWatermark();
      result = 'success';
      return ret;
    } finally {
      litestreamBackupVerificationDuration().recordMs(
        performance.now() - start,
        {
          ...litestreamMonitorMetricAttrs(
            this.#backupURL,
            'v5',
            'replication_manager',
          ),
          result,
        },
      );
    }
  }

  #scheduleCleanup(): void {
    const latestConfirmedWatermark = this.#latestBackupWatermark?.watermark;
    if (latestConfirmedWatermark === undefined) {
      return;
    }

    const maxWatermark = this.#maxWatermarkUpTo(
      Date.now(),
      latestConfirmedWatermark,
    );
    if (maxWatermark.length === 0) {
      return;
    }

    this.#changeStreamer.trackBackupWatermark(maxWatermark);
    for (const watermark of this.#watermarks.keys()) {
      if (watermark <= maxWatermark) {
        this.#watermarks.delete(watermark);
      }
    }
    this.#lastWatermark = maxWatermark;
  }

  #maxWatermarkUpTo(cutoff: number, latestConfirmedWatermark: string): string {
    let max = '';
    for (const [watermark, backupWatermark] of this.#watermarks) {
      if (
        watermark <= latestConfirmedWatermark &&
        backupWatermark.observedAtMs <= cutoff &&
        watermark > max
      ) {
        max = watermark;
      }
    }
    return max;
  }

  stop(): Promise<void> {
    clearInterval(this.#checkWatermarkTimer);
    this.#source.close?.();
    this.#state.stop(this.#lc);
    return promiseVoid;
  }

  #initBackupLagMetric(): void {
    getOrCreateGauge('replica', 'backup_lag', {
      description:
        'Latency from when a change is written to the replica ' +
        'to when it is backed up to litestream.',
      unit: 'millisecond',
    }).addCallback(o => {
      const latestBackupWatermark = this.#latestBackupWatermark;
      if (latestBackupWatermark?.writeTimeMs === undefined) {
        this.#lc.warn?.(
          `no backed up watermarks. unable to report replica.backup_lag`,
        );
        return;
      }
      if (latestBackupWatermark.writeTimeMs === null) {
        return;
      }
      o.observe(
        Math.max(
          0,
          latestBackupWatermark.observedAtMs -
            latestBackupWatermark.writeTimeMs,
        ),
      );
    });
  }
}
