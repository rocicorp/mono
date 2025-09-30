import type {OnlineManager} from './online-manager.ts';

/**
 * Client-side error thrown when the Zero client is in offline mode.
 * Offline mode is a local state that disables mutators until connectivity
 * is restored (or the mode is cancelled).
 *
 * @see {@link https://zero.rocicorp.dev/docs/offline Offline mode docs}
 */
export class OfflineError extends Error {
  readonly name = 'OfflineError';
  readonly code = 'OFFLINE';
  constructor() {
    super(
      `Offline mode: mutations are disabled while offline. See https://zero.rocicorp.dev/docs/offline for more information.`,
    );
  }
}

export const offlinePromiseRejection = (
  onlineManager: OnlineManager | undefined,
): Promise<never> | undefined => {
  if (onlineManager?.status === 'offline') {
    return Promise.reject(new OfflineError());
  }
  return undefined;
};
