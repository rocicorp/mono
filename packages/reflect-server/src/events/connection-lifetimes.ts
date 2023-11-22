import type {AlarmScheduler} from '../server/alarms.js';
import type {ConnectionCountTracker} from '../types/client-state.js';

export const CONNECTION_CLOSED_FLUSH_INTERVAL_MS = 20 * 1000;

export class ConnectionLifetimeReporter implements ConnectionCountTracker {
  readonly #scheduler: AlarmScheduler;
  #currentCount: number = 0;

  constructor(scheduler: AlarmScheduler) {
    this.#scheduler = scheduler;
  }

  onConnectionCountChange(currentCount: number): void {
    const prevCount = this.#currentCount;
    this.#currentCount = currentCount;

    // The sole purpose of this class is to schedule a TailEvent (via an Alarm)
    // after a connection closes. This prevents the FetchEvents corresponding
    // to the closed connection from being buffered, since that delays the time
    // at which our tail worker receives the FetchEvents and results in
    // overcounting the lifetime of the connection.
    if (this.#currentCount < prevCount) {
      this.#scheduler.setTimeout(lc => {
        lc.info?.('Empty Alarm to flush FetchEvent of closed connection');
      }, CONNECTION_CLOSED_FLUSH_INTERVAL_MS);
    }
  }
}
