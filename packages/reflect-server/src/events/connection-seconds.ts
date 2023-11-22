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

export class ConnectionSecondsReporter implements ConnectionCountTracker {
  readonly #channel: Channel;
  readonly #scheduler: AlarmScheduler;

  #timeoutID: Promise<TimeoutID> = Promise.resolve(0);
  #connectionMs: number = 0;
  #roomMs: number = 0;
  #currentCount: number = 0;
  #lastCountChange: number = 0;
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
    this.#connectionMs += this.#currentCount * (now - this.#lastCountChange);
    if (this.#currentCount > 0) {
      this.#roomMs += now - this.#lastCountChange;
    }
    this.#currentCount = currentCount;
    this.#lastCountChange = now;

    if (flush) {
      // If the roomID has not yet been set, wait for the next interval to flush.
      if (this.#roomID !== undefined) {
        const period = this.#roomMs / 1000;
        const elapsed = this.#connectionMs / 1000;

        const report: ConnectionSecondsReport = {
          period,
          elapsed,
          roomID: this.#roomID,
        };
        this.#channel.publish(report);
        this.#connectionMs = 0;
        this.#roomMs = 0;
      }
      this.#timeoutID = Promise.resolve(0);
    }

    // After updating the bookkeeping state, the next timeout is scheduled.
    // Scheduling state is kept consistent by serializing updates on the
    // `#timeoutID` Promise.
    this.#timeoutID = this.#timeoutID.then(timeoutID =>
      this.#scheduleFlush(timeoutID, prevCount, currentCount),
    );
    await this.#timeoutID;
  }

  async #scheduleFlush(
    currTimeoutID: TimeoutID,
    prevCount: number,
    currentCount: number,
  ): Promise<TimeoutID> {
    // When the last connection closes, schedule an immediate flush so that the
    // timings are reported before the RoomDO is shutdown.
    const fastFlush = currentCount < prevCount && currentCount === 0;
    if (!fastFlush) {
      if (
        currTimeoutID !== 0 || // Keep the existing schedule.
        currentCount === 0 // Nothing to flush.
      ) {
        return currTimeoutID;
      }
    }
    const newTimeoutID = await this.#scheduler.promiseTimeout(
      lc => this.#flush(lc),
      fastFlush ? 0 : REPORTING_INTERVAL_MS,
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
