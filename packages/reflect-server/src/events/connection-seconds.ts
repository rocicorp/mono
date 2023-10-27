import type {LogContext} from '@rocicorp/logger';
import {channel, Channel} from 'node:diagnostics_channel';
import {
  CONNECTION_SECONDS_CHANNEL_NAME,
  type ConnectionSecondsReport,
} from 'shared/src/events/connection-seconds.js';
import type {AlarmScheduler, TimeoutID} from '../server/alarms.js';
import type {ConnectionCountTracker} from '../types/client-state.js';

// Normal reporting interval.
export const REPORTING_INTERVAL_MS = 60 * 1000;

// Shorter flush interval when the number of connections drops.
export const CONNECTION_CLOSED_FLUSH_INTERVAL_MS = 10 * 1000;

export class ConnectionSecondsReporter implements ConnectionCountTracker {
  readonly #channel: Channel;
  readonly #scheduler: AlarmScheduler;

  #timeoutID: TimeoutID = 0;
  #elapsedMs: number = 0;
  #currentCount: number = 0;
  #lastCountChange: number = 0;
  #intervalStartTime: number = 0;

  constructor(
    scheduler: AlarmScheduler,
    diagnosticChannelName = CONNECTION_SECONDS_CHANNEL_NAME, // Overridden in test for isolation
  ) {
    this.#channel = channel(diagnosticChannelName);
    this.#scheduler = scheduler;
  }

  async onConnectionCountChange(currentCount: number): Promise<void> {
    await this.#update(currentCount, false);
  }

  async #update(currentCount: number, flush: boolean): Promise<void> {
    // Note: All bookkeeping variables must be updated before the (blocking)
    // scheduling to ensure that they are atomicity updated.
    const now = Date.now();

    const prevCount = this.#currentCount;
    this.#elapsedMs += this.#currentCount * (now - this.#lastCountChange);
    this.#currentCount = currentCount;
    this.#lastCountChange = now;

    if (flush) {
      const interval = (now - this.#intervalStartTime) / 1000;
      const elapsed = this.#elapsedMs / 1000;

      const report: ConnectionSecondsReport = {interval, elapsed};
      this.#channel.publish(report);

      this.#elapsedMs = 0;
      this.#timeoutID = 0;
    }

    // After updating the bookkeeping state, the next timeout is scheduled.
    // Note that this step is not atomic, but the worst that can happen is
    // the scheduling of a redundant alarm.
    if (currentCount < prevCount) {
      // When a connection closes, schedule an earlier flush so that (1) the FetchEvents
      // that correspond to the closed websocket are immediately flushed to the tail log
      // and (2) in the case that there are no longer any connections, we report the connection
      // times before the DO is shut down.
      await this.#scheduleFlush(CONNECTION_CLOSED_FLUSH_INTERVAL_MS, now);
    } else if (currentCount > 0 && this.#timeoutID === 0) {
      // currentCount moves from 0 to non-zero. Schedule a new timeout.
      await this.#scheduleFlush(REPORTING_INTERVAL_MS, now);
    }
  }

  async #scheduleFlush(intervalMs: number, now: number): Promise<void> {
    const prevTimeoutID = this.#timeoutID;
    if (prevTimeoutID === 0) {
      this.#intervalStartTime = now;
    }
    this.#timeoutID = await this.#scheduler.promiseTimeout(
      lc => this.#flush(lc),
      intervalMs,
    );
    // Optimization: Because rescheduling is always to an earlier timeout,
    // schedule the new (earlier) timeout first before clearing the later one.
    // This avoids unnecessarily clearing the DO Alarm, or setting it later, before
    // setting the earlier Alarm.
    if (prevTimeoutID !== 0) {
      await this.#scheduler.clearTimeout(prevTimeoutID);
    }
  }

  async #flush(lc: LogContext): Promise<void> {
    lc.info?.(`Flushing connection seconds`);
    await this.#update(this.#currentCount, true);
  }
}
