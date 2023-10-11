import {channel, Channel} from 'node:diagnostics_channel';
import {
  type ConnectionSecondsReport,
  CONNECTION_SECONDS_CHANNEL_NAME,
} from 'shared/src/events/connection-seconds.js';
import type {AlarmScheduler, TimeoutID} from '../server/alarms.js';
import type {LogContext} from '@rocicorp/logger';

export interface ConnectionCountTracker {
  onConnectionCountChange(currentCount: number): Promise<void>;
}

export const REPORTING_INTERVAL_SECONDS = 15;

export class ConnectionSecondsReporter implements ConnectionCountTracker {
  readonly #channel: Channel;
  readonly #scheduler: AlarmScheduler;

  #timeoutID: TimeoutID = 0;
  #elapsedMs: number = 0;
  #currentCount: number = 0;
  #lastCountChange: number = 0;

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

  async #update(currentCount: number, resetElapsed: boolean): Promise<number> {
    const now = Date.now();

    this.#elapsedMs += this.#currentCount * (now - this.#lastCountChange);
    this.#currentCount = currentCount;
    this.#lastCountChange = now;

    if (currentCount > 0 && this.#timeoutID === 0) {
      this.#timeoutID = await this.#scheduler.promiseTimeout(
        lc => this.#flush(lc),
        REPORTING_INTERVAL_SECONDS * 1000,
      );
    }

    const elapsedMs = this.#elapsedMs;
    if (resetElapsed) {
      this.#elapsedMs = 0;
    }
    return elapsedMs;
  }

  async #flush(lc: LogContext): Promise<void> {
    this.#timeoutID = 0; // Clear the TimeoutID so that it can be rescheduled as necessary.
    const elapsedMs = await this.#update(this.#currentCount, true);
    const elapsed = elapsedMs / 1000;

    lc.info?.(`Flushing connection seconds ${elapsed}`);
    const report: ConnectionSecondsReport = {
      interval: REPORTING_INTERVAL_SECONDS,
      elapsed,
    };
    this.#channel.publish(report);
  }
}
