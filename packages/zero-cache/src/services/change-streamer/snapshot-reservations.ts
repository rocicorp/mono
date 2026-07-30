import type {LogContext} from '@rocicorp/logger';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {
  litestreamMonitorMetricAttrs,
  litestreamSnapshotReservationDuration,
} from '../litestream/metrics.ts';
import type {BackupConfig} from './change-streamer-service.ts';
import type {SnapshotMessage} from './snapshot.ts';

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
      litestreamSnapshotReservationDuration().recordMs(duration, {
        ...litestreamMonitorMetricAttrs(
          this.#backupConfig.backupURL,
          this.#backupConfig.litestreamVersion,
          'view_syncer',
        ),
        result:
          cancelledInstanceID || !res.confirmed() ? 'cancelled' : 'closed',
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

  confirm(replicaVersion: string, minWatermark: string) {
    for (const [taskID, res] of this.#reservations.entries()) {
      if (!res.confirmed()) {
        this.#lc.info?.(
          `reserving change-log entries since ${minWatermark} for ${taskID}`,
        );
        res.confirm(this.#backupConfig.backupURL, replicaVersion, minWatermark);
      }
    }
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
