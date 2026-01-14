import type {LogContext} from '@rocicorp/logger';
import parsePrometheusTextFormat from 'parse-prometheus-text-format';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import {Subscription} from '../../types/subscription.ts';
import {RunningState} from '../running-state.ts';
import type {Service} from '../service.ts';
import type {ChangeStreamerService} from './change-streamer.ts';
import type {SnapshotMessage} from './snapshot.ts';

export const CHECK_INTERVAL_MS = 60 * 1000;
const MIN_CLEANUP_DELAY_MS = 30 * 1000;

type Reservation = {
  start: Date;
  sub: Subscription<SnapshotMessage>;
  timeout: NodeJS.Timeout | undefined;
};

/**
 * The BackupMonitor polls the litestream "/metrics" endpoint to track the
 * watermark (label) value of the `litestream_replica_progress` gauge and
 * schedules cleanup of change log entries that can be purged as a result.
 *
 * See: https: *github.com/rocicorp/litestream/pull/3
 *
 * Note that change log entries cannot simply be purged as soon as they
 * have been applied and backed up by litestream. Consider the case in which
 * litestream backs up new wal segments every minute, but it takes 5 minutes
 * to restore a replica: if a zero-cache starts restoring a replica at
 * minute 0, and new watermarks are replicated at minutes 1, 2, 3, 4, and 5,
 * purging changelog records as soon as those watermarks are replicated would
 * result in the zero-cache not being able to catch up from minute 0 once it
 * has finished restoring the replica.
 *
 * The `/snapshot` reservation protocol is used to prevent premature change
 * log cleanup:
 * - Clients restoring a snapshot initiate a `/snapshot` request and hold that
 *   request open while it restores its snapshot, prepares it, and
 *   starts its subscription to the change stream. During this time, no
 *   cleanups are scheduled.
 * - When the subscription is started, the interval since the beginning of
 *   of the reservation is tracked to increase the background cleanup delay
 *   interval if needed. The reservation is ended (and request closed), and
 *   cleanup scheduling is resumed with the current delay interval.
 *
 * Note that the reservation request is the primary mechanism by which
 * premature change log cleanup is prevented. The cleanup delay interval is
 * a secondary safeguard.
 */
export class BackupMonitor implements Service {
  readonly id = 'backup-monitor';
  readonly #lc: LogContext;
  readonly #backupURL: string;
  readonly #metricsEndpoint: string;
  readonly #changeStreamer: ChangeStreamerService;
  readonly #state = new RunningState(this.id);

  readonly #reservations = new Map<string, Reservation>();
  readonly #watermarks = new Map<string, Date>();

  #lastWatermark: string = '';
  #cleanupDelayMs: number;
  readonly #snapshotReservationTimeoutMs: number;
  #checkMetricsTimer: NodeJS.Timeout | undefined;

  constructor(
    lc: LogContext,
    backupURL: string,
    metricsEndpoint: string,
    changeStreamer: ChangeStreamerService,
    initialCleanupDelayMs: number,
    snapshotReservationTimeoutMs: number,
  ) {
    this.#lc = lc.withContext('component', this.id);
    this.#backupURL = backupURL;
    this.#metricsEndpoint = metricsEndpoint;
    this.#changeStreamer = changeStreamer;
    this.#cleanupDelayMs = Math.max(
      initialCleanupDelayMs,
      MIN_CLEANUP_DELAY_MS, // purely for peace of mind
    );
    this.#snapshotReservationTimeoutMs = snapshotReservationTimeoutMs;

    this.#lc.info?.(
      `backup monitor started ${initialCleanupDelayMs} ms after snapshot restore`,
    );
  }

  run(): Promise<void> {
    this.#lc.info?.(
      `monitoring backups at ${this.#metricsEndpoint} with ` +
        `${this.#cleanupDelayMs} ms cleanup delay`,
    );
    this.#checkMetricsTimer = setInterval(
      this.checkWatermarksAndScheduleCleanup,
      CHECK_INTERVAL_MS,
    );
    return this.#state.stopped();
  }

  async startSnapshotReservation(
    taskID: string,
  ): Promise<Subscription<SnapshotMessage>> {
    this.#lc.info?.(`pausing change-log cleanup while ${taskID} snapshots`);
    // In the case of retries, only track the last reservation.
    this.#reservations.get(taskID)?.sub.cancel();

    const sub = Subscription.create<SnapshotMessage>({
      // If the reservation still exists when the connection closes
      // (e.g. subscriber crashed), clean it up without updating the
      // cleanup delay.
      cleanup: () => this.endReservation(taskID, false),
    });
    const timeout =
      this.#snapshotReservationTimeoutMs > 0
        ? setTimeout(() => {
            if (this.#reservations.has(taskID)) {
              this.#lc.warn?.(
                `snapshot reservation for ${taskID} exceeded timeout ` +
                  `(${this.#snapshotReservationTimeoutMs} ms). Ending reservation.`,
              );
            }
            this.endReservation(taskID, false);
          }, this.#snapshotReservationTimeoutMs)
        : undefined;

    this.#reservations.set(taskID, {start: new Date(), sub, timeout});
    const changeLogState = await this.#changeStreamer.getChangeLogState();
    sub.push([
      'status',
      {tag: 'status', backupURL: this.#backupURL, ...changeLogState},
    ]);
    return sub;
  }

  endReservation(taskID: string, updateCleanupDelay = true) {
    const res = this.#reservations.get(taskID);
    if (res === undefined) {
      return;
    }
    this.#reservations.delete(taskID);
    const {start, sub, timeout} = res;

    clearTimeout(timeout);
    sub.cancel(); // closes the connection if still open

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
  readonly checkWatermarksAndScheduleCleanup = async () => {
    try {
      await this.#checkWatermarks();
    } catch (e) {
      this.#lc.warn?.(`unable to fetch metrics at ${this.#metricsEndpoint}`, e);
    }
    try {
      this.#scheduleCleanup();
    } catch (e) {
      this.#lc.warn?.(`error scheduling cleanup`, e);
    }
  };

  async #checkWatermarks() {
    const resp = await fetch(this.#metricsEndpoint);
    if (!resp.ok) {
      this.#lc.warn?.(
        `unable to fetch metrics at ${this.#metricsEndpoint}`,
        await resp.text(),
      );
      return;
    }
    const families = parsePrometheusTextFormat(await resp.text());
    for (const family of families) {
      if (
        family.type === 'GAUGE' &&
        family.name === 'litestream_replica_progress'
      ) {
        for (const metric of family.metrics) {
          const watermark = metric.labels?.watermark;
          if (
            watermark &&
            watermark > this.#lastWatermark &&
            !this.#watermarks.has(watermark)
          ) {
            const time = new Date(parseFloat(metric.value) * 1000);
            this.#lc.info?.(
              `replicated watermark=${watermark} to ${metric.labels?.name}` +
                ` at ${time.toISOString()}.`,
            );
            this.#watermarks.set(watermark, time);
          }
        }
      }
    }
  }

  #scheduleCleanup() {
    if (this.#reservations.size > 0) {
      this.#lc.info?.(
        `watermark cleanup paused for snapshot(s): ${[...this.#reservations.keys()]}`,
      );
      return;
    }
    const latestCleanupTime = Date.now() - this.#cleanupDelayMs;
    let maxWatermark = '';
    for (const [watermark, backupTime] of this.#watermarks.entries()) {
      if (
        backupTime.getTime() <= latestCleanupTime &&
        watermark > maxWatermark
      ) {
        maxWatermark = watermark;
      }
    }
    if (maxWatermark.length) {
      this.#changeStreamer.scheduleCleanup(maxWatermark);
      for (const watermark of this.#watermarks.keys()) {
        if (watermark <= maxWatermark) {
          this.#watermarks.delete(watermark);
        }
      }
      this.#lastWatermark = maxWatermark;
    }
  }

  stop(): Promise<void> {
    clearInterval(this.#checkMetricsTimer);
    for (const taskID of [...this.#reservations.keys()]) {
      // Close any pending reservations. This commonly happens when a new
      // replication-manager makes a `/snapshot` reservation on the existing
      // replication-manager, and then shuts it down when it takes over the
      // replication slot.
      this.endReservation(taskID, false);
    }
    this.#state.stop(this.#lc);
    return promiseVoid;
  }
}
