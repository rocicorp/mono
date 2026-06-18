import type {Subscription} from '../../types/subscription.ts';
import type {SingletonService} from '../service.ts';
import type {SnapshotMessage} from './snapshot.ts';

export interface SnapshotReservationMonitor {
  startSnapshotReservation(taskID: string): Subscription<SnapshotMessage>;
  endReservation(
    taskID: string,
    updateCleanupDelay?: boolean | undefined,
  ): void;
}

export interface BackupCleanupMonitor
  extends SingletonService, SnapshotReservationMonitor {}
