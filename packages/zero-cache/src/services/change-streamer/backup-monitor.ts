import type {SingletonService} from '../service.ts';

export interface BackupMonitor extends SingletonService {
  // TODO: Add a method for delaying readiness until a backup is available.
  // backupAvailable(): Promise<void>;
}
