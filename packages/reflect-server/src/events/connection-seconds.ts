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

  #timeoutID: Promise<TimeoutID> = Promise.resolve(0);
  #elapsedMs: number = 0;
  #currentCount: number = 0;
  #lastCountChange: number = 0;
  #intervalStartTime: number = 0;
  #roomID: string | undefined;

  constructor(
    scheduler: AlarmScheduler,
    diagnosticChannelName = CONNECTION_SECONDS_CHANNEL_NAME, // Overridden in test for isolation
  ) {
    this.#channel = channel(diagnosticChannelName);
    this.#scheduler = scheduler;
  }

  setRoomID(roomID: string) {
    this.#roomID = roomID;
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
      // If the roomID has not yet been set, wait for the next interval to flush.
      if (this.#roomID !== undefined) {
        const period = (now - this.#intervalStartTime) / 1000;
        const elapsed = this.#elapsedMs / 1000;

        const report: ConnectionSecondsReport = {
          period,
          elapsed,
          roomID: this.#roomID,
        };
        this.#channel.publish(report);
        this.#elapsedMs = 0;
      }
      this.#timeoutID = Promise.resolve(0);
    }

    // After updating the bookkeeping state, the next timeout is scheduled.
    // Scheduling state is kept consistent by serializing updates on the
    // `#timeoutID` Promise.
    this.#timeoutID = this.#timeoutID.then(timeoutID =>
      this.#scheduleFlush(timeoutID, prevCount, currentCount, now),
    );
    await this.#timeoutID;
  }

  async #scheduleFlush(
    currTimeoutID: TimeoutID,
    prevCount: number,
    currentCount: number,
    now: number,
  ): Promise<TimeoutID> {
    // When a connection closes, schedule an earlier flush so that (1) the FetchEvents
    // that correspond to the closed websocket are immediately flushed to the tail log
    // and (2) in the case that there are no longer any connections, we report the connection
    // times before the DO is shut down.
    const fastFlush = currentCount < prevCount;
    if (!fastFlush) {
      if (
        currTimeoutID !== 0 || // Keep the existing schedule.
        currentCount === 0 // Nothing to flush.
      ) {
        return currTimeoutID;
      }
    }
    if (currTimeoutID === 0) {
      this.#intervalStartTime = now;
    }
    const newTimeoutID = await this.#scheduler.promiseTimeout(
      lc => this.#flush(lc),
      fastFlush ? CONNECTION_CLOSED_FLUSH_INTERVAL_MS : REPORTING_INTERVAL_MS,
    );
    // Optimization: Because rescheduling is always to an earlier timeout,
    // schedule the new (earlier) timeout first before clearing the later one.
    // This avoids unnecessarily clearing the DO Alarm, or setting it later, before
    // setting the earlier Alarm.
    if (currTimeoutID !== 0) {
      await this.#scheduler.clearTimeout(currTimeoutID);
    }
    return newTimeoutID;
  }

  async #flush(lc: LogContext): Promise<void> {
    lc.info?.(`Flushing connection seconds`);
    await this.#update(this.#currentCount, true);
  }
}
