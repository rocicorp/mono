import type {LogContext} from '@rocicorp/logger';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import {
  getOrCreateCounter,
  getOrCreateGauge,
} from '../../observability/metrics.ts';
import {majorVersionFromString} from '../../types/state-version.ts';
import {Subscription} from '../../types/subscription.ts';
import type {VfsBackupWatermark} from '../litestream/vfs-watermark-reader.ts';
import {RunningState} from '../running-state.ts';
import type {BackupMonitor} from './backup-monitor.ts';
import type {ChangeStreamerService} from './change-streamer.ts';
import type {SnapshotMessage} from './snapshot.ts';

const MIN_CLEANUP_DELAY_MS = 30_000;

type Reservation = {
  start: Date;
  sub: Subscription<SnapshotMessage>;
};

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

  readonly #reservations = new Map<string, Reservation>();
  readonly #watermarks = new Map<string, VfsBackupWatermark>();

  readonly #purgesBlocked = getOrCreateCounter('replica', 'purge_blocked', {
    description:
      'Number of change-log purges blocked because the actual backup ' +
      'watermark could not be read through the Litestream VFS.',
  });

  #lastWatermark: string = '';
  #latestBackupWatermark: VfsBackupWatermark | undefined;
  #cleanupDelayMs: number;
  #checkWatermarkTimer: NodeJS.Timeout | undefined;

  constructor(
    lc: LogContext,
    backupURL: string,
    changeStreamer: ChangeStreamerService,
    initialCleanupDelayMs: number,
    probeIntervalMs: number,
    source: VfsBackupWatermarkSource,
  ) {
    this.#lc = lc.withContext('component', this.id);
    this.#backupURL = backupURL;
    this.#changeStreamer = changeStreamer;
    this.#source = source;
    this.#probeIntervalMs = probeIntervalMs;
    this.#cleanupDelayMs = Math.max(
      initialCleanupDelayMs,
      MIN_CLEANUP_DELAY_MS,
    );
  }

  run(): Promise<void> {
    this.#lc.info?.(
      `monitoring v5 backups at ${this.#backupURL} with ` +
        `${this.#cleanupDelayMs} ms cleanup delay`,
    );
    this.#checkWatermarkTimer = setInterval(
      this.checkWatermarkAndScheduleCleanup,
      this.#probeIntervalMs,
    );
    this.#initBackupLagMetric();
    this.#initShadowAckLagMetric();
    return this.#state.stopped();
  }

  startSnapshotReservation(taskID: string): Subscription<SnapshotMessage> {
    this.#lc.info?.(`pausing change-log cleanup while ${taskID} snapshots`);
    this.#reservations.get(taskID)?.sub.cancel();

    const sub = Subscription.create<SnapshotMessage>({
      cleanup: () => this.endReservation(taskID, false),
    });
    this.#reservations.set(taskID, {start: new Date(), sub});

    void this.#changeStreamer
      .getChangeLogState()
      .then(changeLogState => {
        sub.push([
          'status',
          {tag: 'status', backupURL: this.#backupURL, ...changeLogState},
        ]);
      })
      .catch(e => {
        this.#lc.warn?.(`failing snapshot reservation`, e);
        sub.fail(e);
      });
    return sub;
  }

  endReservation(taskID: string, updateCleanupDelay = true): void {
    const res = this.#reservations.get(taskID);
    if (res === undefined) {
      return;
    }
    this.#reservations.delete(taskID);
    const {start, sub} = res;
    sub.cancel();

    if (updateCleanupDelay) {
      const duration = Date.now() - start.getTime();
      this.#lc.info?.(`snapshot initialized by ${taskID} in ${duration} ms`);
      if (duration > this.#cleanupDelayMs) {
        this.#cleanupDelayMs = duration;
        this.#lc.info?.(`increased cleanup delay to ${duration} ms`);
      }
    }
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
    const watermark = await this.#source.readWatermark();
    this.#latestBackupWatermark = watermark;
    // Report the backed-up watermark so the change-streamer can (when
    // --litestream-ack-from-backup is set) cap the slot ACK at it.
    this.#changeStreamer.onBackupWatermark(watermark.watermark);
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
          consumedWatermark: this.#changeStreamer.getLastConsumedWatermark(),
          shadowAckLagBytes: this.#shadowAckLagBytes(),
        },
      );
      this.#watermarks.set(watermark.watermark, watermark);
    }
  }

  #scheduleCleanup(): void {
    if (this.#reservations.size > 0) {
      this.#lc.info?.(
        `watermark cleanup paused for snapshot(s): ${[...this.#reservations.keys()]}`,
      );
      return;
    }

    const latestConfirmedWatermark = this.#latestBackupWatermark?.watermark;
    if (latestConfirmedWatermark === undefined) {
      return;
    }

    const maxWatermark = this.#maxWatermarkUpTo(
      Date.now() - this.#cleanupDelayMs,
      latestConfirmedWatermark,
    );
    if (maxWatermark.length === 0) {
      return;
    }

    this.#changeStreamer.scheduleCleanup(maxWatermark);
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
    for (const {sub} of this.#reservations.values()) {
      sub.cancel();
    }
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

  /**
   * The number of WAL bytes the replication slot would *additionally* retain
   * if it were ACKed from the litestream backup watermark instead of from
   * change-log durability. LSN deltas are byte offsets in the WAL, and the
   * change-log-durable watermark (≈ stream head) is normally at or ahead of
   * the backup watermark, so the difference is the extra retention RMv2's
   * backup-driven ACK would impose. Returns `undefined` until both watermarks
   * are known; clamps to 0 to ignore transient skew.
   */
  #shadowAckLagBytes(): number | undefined {
    const backupWatermark = this.#latestBackupWatermark?.watermark;
    const consumedWatermark = this.#changeStreamer.getLastConsumedWatermark();
    if (!backupWatermark || !consumedWatermark) {
      return undefined;
    }
    const lag =
      majorVersionFromString(consumedWatermark) -
      majorVersionFromString(backupWatermark);
    return Number(lag > 0n ? lag : 0n);
  }

  #initShadowAckLagMetric(): void {
    getOrCreateGauge('replica', 'shadow_ack_lag_bytes', {
      description:
        'WAL bytes the replication slot would additionally retain if it were ' +
        'ACKed from the litestream backup watermark instead of change-log ' +
        'durability. Observation-only; sizes WAL retention ahead of the ' +
        'cutover to backup-driven slot ACKs (RMv2).',
      unit: 'byte',
    }).addCallback(o => {
      const lagBytes = this.#shadowAckLagBytes();
      if (lagBytes !== undefined) {
        o.observe(lagBytes);
      }
    });
  }
}
