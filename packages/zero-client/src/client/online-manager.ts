import {Subscribable} from '../../../shared/src/subscribable.ts';
import type {ZeroLogContext} from './zero-log-context.ts';

export type OnlineStatus = 'online' | 'offline' | 'offline-pending';

export class OnlineManager extends Subscribable<OnlineStatus> {
  // we start in online mode to assume connectivity, then
  // update when Zero attempts connection
  #status: OnlineStatus = 'online';

  #offlineDelayMs: number;
  #pendingOfflineTimer: ReturnType<typeof setTimeout> | undefined;
  #lc: ZeroLogContext;

  constructor(offlineDelayMs: number, lc: ZeroLogContext) {
    super();
    this.#offlineDelayMs = offlineDelayMs;
    this.#lc = lc;
  }

  setOnline(online: boolean): void {
    if (this.#status === (online ? 'online' : 'offline')) {
      return;
    }

    if (online) {
      if (this.#pendingOfflineTimer !== undefined) {
        clearTimeout(this.#pendingOfflineTimer);
        this.#pendingOfflineTimer = undefined;
      }
      this.#setStatus('online');
      return;
    }

    if (this.#pendingOfflineTimer === undefined) {
      this.#lc.debug?.(
        'Scheduling offline mode in',
        this.#offlineDelayMs,
        'ms',
      );

      this.#setStatus('offline-pending');

      this.#pendingOfflineTimer = setTimeout(() => {
        this.#pendingOfflineTimer = undefined;
        this.#setStatus('offline');
        this.#lc.info?.('Offline mode enabled');
      }, this.#offlineDelayMs);
    }
  }

  #setStatus(status: OnlineStatus): void {
    this.#status = status;
    this.notify(this.#status);
  }

  get status(): OnlineStatus {
    return this.#status;
  }
}
