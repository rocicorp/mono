import type {LogContext} from '@rocicorp/logger';
import {assert} from './asserts.js';

export class BufferSizer {
  private _bufferSizeMs: number;
  private readonly _initialBufferSizeMs: number;
  private readonly _minBufferSizeMs: number;
  private readonly _maxBufferSizeMs: number;
  private readonly _adjustBufferSizeIntervalMs: number;
  private _bufferNeededMsHistory: number[] = [];
  private _missableCountSinceLastBufferAdjust = 0;
  private _missedCountSinceLastBufferAdjust = 0;
  private _timeOfLastBufferAdjust = -1;
  private _ignoreNextMissable = false;

  constructor(options: {
    initialBufferSizeMs: number;
    minBuferSizeMs: number;
    maxBufferSizeMs: number;
    adjustBufferSizeIntervalMs: number;
  }) {
    assert(options.minBuferSizeMs <= options.maxBufferSizeMs);
    assert(options.initialBufferSizeMs >= options.minBuferSizeMs);
    assert(options.initialBufferSizeMs <= options.maxBufferSizeMs);
    assert(options.adjustBufferSizeIntervalMs > 0);
    this._initialBufferSizeMs = options.initialBufferSizeMs;
    this._minBufferSizeMs = options.minBuferSizeMs;
    this._maxBufferSizeMs = options.maxBufferSizeMs;
    this._adjustBufferSizeIntervalMs = options.adjustBufferSizeIntervalMs;
    this._bufferSizeMs = this._initialBufferSizeMs;
  }

  get bufferSizeMs() {
    return this._bufferSizeMs;
  }

  recordMissable(
    now: number,
    missed: boolean,
    bufferNeededMs: number,
    lc: LogContext,
  ) {
    if (this._ignoreNextMissable) {
      this._ignoreNextMissable = false;
      return;
    }

    lc = lc.addContext('BufferSizer');
    this._bufferNeededMsHistory.push(bufferNeededMs);
    this._missableCountSinceLastBufferAdjust++;
    if (missed) {
      this._missedCountSinceLastBufferAdjust++;
    }
    if (this._timeOfLastBufferAdjust === -1) {
      this._timeOfLastBufferAdjust = now;
      return;
    }
    if (now - this._timeOfLastBufferAdjust < this._adjustBufferSizeIntervalMs) {
      return;
    }
    if (this._missableCountSinceLastBufferAdjust < 200) {
      return;
    }

    this._bufferNeededMsHistory.sort((a, b) => a - b);
    const targetBufferNeededMs =
      this._bufferNeededMsHistory[
        Math.floor((this._bufferNeededMsHistory.length * 99.5) / 100)
      ];
    const bufferSizeMs = this._bufferSizeMs;

    lc.debug?.(
      'bufferSizeMs',
      bufferSizeMs,
      'targetBufferNeededMs',
      targetBufferNeededMs,
      'this._maxBufferNeededMs.length',
      this._bufferNeededMsHistory.length,
      'percentile index',
      Math.floor((this._bufferNeededMsHistory.length * 99.5) / 100),
      this._bufferNeededMsHistory,
    );
    let newBufferSizeMs = bufferSizeMs;
    const missPercent =
      this._missedCountSinceLastBufferAdjust /
      this._missableCountSinceLastBufferAdjust;
    if (missPercent > 0.01) {
      newBufferSizeMs = Math.min(
        this._maxBufferSizeMs,
        Math.max(bufferSizeMs, targetBufferNeededMs),
      );
      lc.debug?.(
        'High miss percent',
        missPercent,
        'over last',
        now - this._timeOfLastBufferAdjust,
        'ms.',
      );
    } else if (missPercent < 0.005) {
      newBufferSizeMs = Math.max(
        this._minBufferSizeMs,
        Math.min(bufferSizeMs, targetBufferNeededMs),
      );
      lc.debug?.(
        'Low miss percent',
        missPercent,
        'over last',
        now - this._timeOfLastBufferAdjust,
        'ms.',
      );
    }

    if (bufferSizeMs !== newBufferSizeMs) {
      lc.debug?.(
        'Adjusting buffer',
        newBufferSizeMs > bufferSizeMs ? 'up' : 'down',
        'from',
        bufferSizeMs,
        'to',
        newBufferSizeMs,
      );
    }

    this._bufferNeededMsHistory = [];
    this._missableCountSinceLastBufferAdjust = 0;
    this._missedCountSinceLastBufferAdjust = 0;
    this._timeOfLastBufferAdjust = now;
    this._bufferSizeMs = newBufferSizeMs;
    this._ignoreNextMissable = true;
  }

  reset() {
    this._bufferSizeMs = this._initialBufferSizeMs;
    this._bufferNeededMsHistory = [];
    this._missableCountSinceLastBufferAdjust = 0;
    this._missedCountSinceLastBufferAdjust = 0;
    this._timeOfLastBufferAdjust = -1;
    this._ignoreNextMissable = false;
  }
}
