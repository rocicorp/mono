import type {OnlineManager} from './online-manager.ts';

/**
 * Client-side error thrown when the Zero client is in offline mode.
 * Offline mode is a local state that disables mutators until connectivity
 * is restored (or the mode is cancelled).
 */
export class OfflineError extends Error {
  readonly name = 'OfflineError';
  readonly code = 'OFFLINE';
  constructor(message = 'Offline mode: Mutations are disabled while offline') {
    super(message);
  }
}

export function assertNotOffline(onlineManager: OnlineManager): void {
  if (onlineManager.status === 'offline') {
    throw new OfflineError();
  }
}
