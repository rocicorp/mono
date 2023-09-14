import type {LogContext} from '@rocicorp/logger';
import {assert} from './asserts.js';

export class BufferSizer {
  #bufferSizeMs: number;
  readonly #initialBufferSizeMs: number;
  readonly #minBufferSizeMs: number;
  readonly #maxBufferSizeMs: number;
  readonly #adjustBufferSizeIntervalMs: number;
  #bufferNeededMsHistory: number[] = [];
  #missableCountSinceLastBufferAdjust = 0;
  #missedCountSinceLastBufferAdjust = 0;
  #timeOfLastBufferAdjust = -1;
  #ignoreNextMissable = false;

  constructor(options: {
    initialBufferSizeMs: number;
    minBufferSizeMs: number;
    maxBufferSizeMs: number;
    adjustBufferSizeIntervalMs: number;
  }) {
    assert(options.minBufferSizeMs <= options.maxBufferSizeMs);
    assert(options.initialBufferSizeMs >= options.minBufferSizeMs);
    assert(options.initialBufferSizeMs <= options.maxBufferSizeMs);
    assert(options.adjustBufferSizeIntervalMs > 0);
    this.#initialBufferSizeMs = options.initialBufferSizeMs;
    this.#minBufferSizeMs = options.minBufferSizeMs;
    this.#maxBufferSizeMs = options.maxBufferSizeMs;
    this.#adjustBufferSizeIntervalMs = options.adjustBufferSizeIntervalMs;
    this.#bufferSizeMs = this.#initialBufferSizeMs;
  }

  get bufferSizeMs() {
    return this.#bufferSizeMs;
  }

  recordMissable(
    now: number,
    missed: boolean,
    bufferNeededMs: number,
    lc: LogContext,
  ) {
    if (this.#ignoreNextMissable) {
      this.#ignoreNextMissable = false;
      return;
    }

    lc = lc.withContext('BufferSizer');
    this.#bufferNeededMsHistory.push(bufferNeededMs);
    this.#missableCountSinceLastBufferAdjust++;
    if (missed) {
      this.#missedCountSinceLastBufferAdjust++;
    }
    if (this.#timeOfLastBufferAdjust === -1) {
      this.#timeOfLastBufferAdjust = now;
      return;
    }
    if (now - this.#timeOfLastBufferAdjust < this.#adjustBufferSizeIntervalMs) {
      return;
    }
    if (this.#missableCountSinceLastBufferAdjust < 200) {
      return;
    }

    this.#bufferNeededMsHistory.sort((a, b) => a - b);
    const targetBufferNeededMs =
      this.#bufferNeededMsHistory[
        Math.floor((this.#bufferNeededMsHistory.length * 99.5) / 100)
      ];
    const bufferSizeMs = this.#bufferSizeMs;

    lc.info?.(
      'bufferSizeMs',
      bufferSizeMs,
      'targetBufferNeededMs',
      targetBufferNeededMs,
      'this._maxBufferNeededMs.length',
      this.#bufferNeededMsHistory.length,
      'percentile index',
      Math.floor((this.#bufferNeededMsHistory.length * 99.5) / 100),
      this.#bufferNeededMsHistory,
    );
    let newBufferSizeMs = bufferSizeMs;
    const missPercent =
      this.#missedCountSinceLastBufferAdjust /
      this.#missableCountSinceLastBufferAdjust;
    if (missPercent > 0.01) {
      newBufferSizeMs = Math.min(
        this.#maxBufferSizeMs,
        Math.max(bufferSizeMs, targetBufferNeededMs),
      );
      lc.info?.(
        'High miss percent',
        missPercent,
        'over last',
        now - this.#timeOfLastBufferAdjust,
        'ms.',
      );
    } else if (missPercent < 0.005) {
      newBufferSizeMs = Math.max(
        this.#minBufferSizeMs,
        Math.min(bufferSizeMs, targetBufferNeededMs),
      );
      lc.info?.(
        'Low miss percent',
        missPercent,
        'over last',
        now - this.#timeOfLastBufferAdjust,
        'ms.',
      );
    }

    if (bufferSizeMs !== newBufferSizeMs) {
      lc.info?.(
        'Adjusting buffer',
        newBufferSizeMs > bufferSizeMs ? 'up' : 'down',
        'from',
        bufferSizeMs,
        'to',
        newBufferSizeMs,
      );
    }

    this.#bufferNeededMsHistory = [];
    this.#missableCountSinceLastBufferAdjust = 0;
    this.#missedCountSinceLastBufferAdjust = 0;
    this.#timeOfLastBufferAdjust = now;
    this.#bufferSizeMs = newBufferSizeMs;
    this.#ignoreNextMissable = true;
  }

  reset() {
    this.#bufferSizeMs = this.#initialBufferSizeMs;
    this.#bufferNeededMsHistory = [];
    this.#missableCountSinceLastBufferAdjust = 0;
    this.#missedCountSinceLastBufferAdjust = 0;
    this.#timeOfLastBufferAdjust = -1;
    this.#ignoreNextMissable = false;
  }
}
