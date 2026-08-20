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

export class SnapshotReservations {
  readonly #lc: LogContext;
  readonly #backupConfig: BackupConfig;
  readonly #onClose: ((taskID: string) => void) | undefined;
  readonly #reservations = new Map<string, Reservation>();

  constructor(
    lc: LogContext,
    backupConfig: BackupConfig,
    onClose?: ((taskID: string) => void) | undefined,
  ) {
    this.#lc = lc.withContext('component', 'snapshot-reserver');
    this.#backupConfig = backupConfig;
    this.#onClose = onClose;
  }

  open(taskID: string): Source<SnapshotMessage> {
    this.close(taskID);

    const instanceID = {};
    const downstream = Subscription.create<SnapshotMessage>({
      cleanup: () => this.#close(taskID, instanceID),
    });
    this.#reservations.set(taskID, new Reservation(instanceID, downstream));
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

  #close(taskID: string, cancelledInstanceID: InstanceID | undefined) {
    const res = this.#reservations.get(taskID);
    if (
      res &&
      (!cancelledInstanceID || res.instanceID === cancelledInstanceID)
    ) {
      // Note: delete first, so that the reservation is gone when close() is called.
      this.#reservations.delete(taskID);
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
        result: cancelledInstanceID ? 'cancelled' : 'closed',
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
  readonly #downstream: Subscription<SnapshotMessage>;
  #watermark: string | null = null;
  #delayNoted = false;

  constructor(
    instanceID: InstanceID,
    downstream: Subscription<SnapshotMessage>,
  ) {
    this.instanceID = instanceID;
    this.#downstream = downstream;
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
