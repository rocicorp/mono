import type {LogContext} from '@rocicorp/logger';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {
  litestreamMonitorMetricAttrs,
  litestreamSnapshotReservationConfirmDuration,
  litestreamSnapshotReservationDuration,
} from '../litestream/metrics.ts';
import type {BackupConfig} from './change-streamer-service.ts';
import type {SnapshotMessage} from './snapshot.ts';
import type {ChangeLogReadSource} from './sqlite-change-log-read-router.ts';

/**
 * How long a snapshot reservation may hold the change log, by default.
 *
 * A reservation keeps the purge floor -- and, for as long as it is open, the
 * purge scheduler itself, which `startSnapshotReservation` pauses -- from
 * moving past the `minWatermark` a restoring view-syncer was promised. It
 * lives exactly as long as its WebSocket. A client that has *died* is already
 * cleaned up by that socket's liveness pings; one that is alive and never
 * finishes -- a wedged restore, or one slower than anybody expected -- would
 * otherwise pin the log for as long as it stays connected, bounded by nothing
 * but the change log's disk.
 *
 * The cap is deliberately far above any plausible restore. Taking a
 * reservation back hands that follower a log that no longer covers its backup,
 * which costs it a `WatermarkTooOld` and another restore -- and a cap below
 * the time a restore actually takes turns that into a loop. An hour bounds the
 * pin at an hour of changes while leaving even a very large replica room to
 * land.
 */
export const DEFAULT_MAX_RESERVATION_AGE_MS = 60 * 60 * 1000;

export type SnapshotReservationOptions = {
  /**
   * How long a reservation may hold the change log before it is taken back.
   * Defaults to {@link DEFAULT_MAX_RESERVATION_AGE_MS}.
   */
  maxAgeMs?: number | undefined;
  setTimeoutFn?: typeof setTimeout | undefined;
};

export class SnapshotReservations {
  readonly #lc: LogContext;
  readonly #backupConfig: BackupConfig;
  readonly #onClose: ((taskID: string) => void) | undefined;
  readonly #reservations = new Map<string, Reservation>();
  readonly #maxAgeMs: number;
  readonly #setTimeoutFn: typeof setTimeout;

  constructor(
    lc: LogContext,
    backupConfig: BackupConfig,
    onClose?: ((taskID: string) => void) | undefined,
    options: SnapshotReservationOptions = {},
  ) {
    this.#lc = lc.withContext('component', 'snapshot-reserver');
    this.#backupConfig = backupConfig;
    this.#onClose = onClose;
    this.#maxAgeMs = options.maxAgeMs ?? DEFAULT_MAX_RESERVATION_AGE_MS;
    this.#setTimeoutFn = options.setTimeoutFn ?? setTimeout;
  }

  open(taskID: string): Source<SnapshotMessage> {
    this.close(taskID);

    const instanceID = {};
    const downstream = Subscription.create<SnapshotMessage>({
      cleanup: () => this.#close(taskID, instanceID),
    });
    // Armed from `open()` rather than from the confirmation: the purge pause
    // that `startSnapshotReservation` takes begins here, so this is when the
    // log starts being held.
    const expiry = this.#setTimeoutFn(
      () => this.#expire(taskID, instanceID),
      this.#maxAgeMs,
    );
    // An hour-scale timer must not be the thing that keeps a stopping process
    // alive. (Optional because a test may inject a plainer timer.)
    expiry.unref?.();
    this.#reservations.set(
      taskID,
      new Reservation(instanceID, downstream, expiry),
    );
    this.#lc.info?.(`created snasphot reservation for ${taskID}`);
    return downstream;
  }

  close(taskID: string) {
    this.#close(taskID, undefined);
  }

  isCurrent(taskID: string, source: Source<SnapshotMessage>): boolean {
    return this.#reservations.get(taskID)?.owns(source) ?? false;
  }

  #metricAttrs() {
    return litestreamMonitorMetricAttrs(
      this.#backupConfig.backupURL,
      this.#backupConfig.litestreamVersion,
      'view_syncer',
    );
  }

  /**
   * Takes back a reservation that has held the change log for longer than
   * `maxAgeMs`, releasing the purge pause and the floor with it.
   *
   * The follower is not told: its `/snapshot` stream simply ends, which is
   * what the change-streamer does when a task subscribes, and its client
   * proceeds with the bounds it was already given. What it has lost is the
   * guarantee behind them, so its subscription may be answered with
   * `WatermarkTooOld` and it will restore again.
   */
  #expire(taskID: string, instanceID: InstanceID) {
    const res = this.#reservations.get(taskID);
    if (res?.instanceID !== instanceID) {
      return; // already closed, or superseded by a retry for the same task
    }
    this.#lc.warn?.(
      `releasing the snapshot reservation for ${taskID}, which has held the ` +
        `change log for more than ${this.#maxAgeMs}ms. Its restore is no ` +
        `longer covered and may have to be repeated; raise ` +
        `--change-streamer-snapshot-reservation-max-age-ms if restores ` +
        `legitimately take this long.`,
      {reservedWatermark: res.reservedWatermark},
    );
    this.#close(taskID, instanceID, 'expired');
  }

  #close(
    taskID: string,
    cancelledInstanceID: InstanceID | undefined,
    result?: 'expired' | undefined,
  ) {
    const res = this.#reservations.get(taskID);
    if (
      res &&
      (!cancelledInstanceID || res.instanceID === cancelledInstanceID)
    ) {
      // Note: delete first, so that the reservation is gone when close() is called.
      this.#reservations.delete(taskID);
      clearTimeout(res.expiry);
      this.#onClose?.(taskID);
      res.close();

      const duration = Date.now() - res.startTime.getTime();
      this.#lc.info?.(
        `ended snapshot reservation for ${taskID} (${duration} ms)`,
      );
      // `result` reports how the reservation ended and `confirmed` whether it
      // ever received its bounds. They are separate dimensions: a follower
      // that gives up while waiting for a confirmation is
      // `result=cancelled, confirmed=false`, which a single collapsed
      // attribute cannot distinguish from a client that simply went away.
      litestreamSnapshotReservationDuration().recordMs(duration, {
        ...this.#metricAttrs(),
        result: result ?? (cancelledInstanceID ? 'cancelled' : 'closed'),
        confirmed: res.confirmed(),
      });
    }
  }

  confirmationsRequired() {
    for (const res of this.#reservations.values()) {
      if (!res.confirmed()) {
        return true;
      }
    }
    return false;
  }

  unconfirmedTaskIDs(): string[] {
    return [...this.#reservations.entries()]
      .filter(([, reservation]) => !reservation.confirmed())
      .map(([taskID]) => taskID);
  }

  /** Confirms one reservation with the bounds of its pinned read source. */
  confirmFor(
    taskID: string,
    replicaVersion: string,
    minWatermark: string,
    source: ChangeLogReadSource,
  ): void {
    const res = this.#reservations.get(taskID);
    if (res && !res.confirmed()) {
      this.#lc.info?.(
        `reserving change-log entries since ${minWatermark} for ${taskID}`,
      );
      res.confirm(this.#backupConfig.backupURL, replicaVersion, minWatermark);
      // Measured from `open()`, not from the first confirmation attempt: what
      // matters is how long the follower waited before it could restore.
      litestreamSnapshotReservationConfirmDuration().recordMs(
        Date.now() - res.startTime.getTime(),
        {...this.#metricAttrs(), source},
      );
    }
  }

  /**
   * Notes that a confirmation was deferred, returning true only the first time
   * for this reservation. Confirmation is retried on every backup, so an
   * undeduplicated count would measure backups rather than delayed followers.
   */
  noteConfirmationDelayed(taskID: string): boolean {
    return this.#reservations.get(taskID)?.noteDelayed() ?? false;
  }

  getReservedWatermarks() {
    return Array.from(
      this.#reservations.values(),
      ({reservedWatermark}) => reservedWatermark,
    ).filter(watermark => watermark !== null);
  }
}

type InstanceID = {};

class Reservation {
  readonly instanceID: InstanceID;
  readonly startTime: Date = new Date();
  /** Cleared when the reservation ends for any other reason. */
  readonly expiry: ReturnType<typeof setTimeout>;
  readonly #downstream: Subscription<SnapshotMessage>;
  #watermark: string | null = null;
  #delayNoted = false;

  constructor(
    instanceID: InstanceID,
    downstream: Subscription<SnapshotMessage>,
    expiry: ReturnType<typeof setTimeout>,
  ) {
    this.instanceID = instanceID;
    this.#downstream = downstream;
    this.expiry = expiry;
  }

  get reservedWatermark() {
    return this.#watermark;
  }

  confirmed() {
    return this.#watermark !== null;
  }

  owns(source: Source<SnapshotMessage>): boolean {
    return this.#downstream === source;
  }

  noteDelayed(): boolean {
    if (this.#delayNoted) {
      return false;
    }
    this.#delayNoted = true;
    return true;
  }

  confirm(backupURL: string, replicaVersion: string, minWatermark: string) {
    if (this.#watermark === null) {
      if (this.#downstream.active) {
        this.#downstream.push([
          'status',
          {tag: 'status', backupURL, replicaVersion, minWatermark},
        ]);
      }
      this.#watermark = minWatermark;
    }
  }

  close() {
    this.#downstream.cancel();
  }
}
