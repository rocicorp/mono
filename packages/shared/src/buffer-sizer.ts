import type {LogContext} from '@rocicorp/logger';

export class BufferSizer {
  private _bufferSizeMs: number;
  private readonly _initialBufferSizeMs: number;
  private readonly _minBufferSizeMs: number;
  private readonly _maxBufferSizeMs: number;
  private readonly _adjustBufferSizeIntervalMs: number;
  private _maxBufferNeededMs: number[] = [];
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
    this._maxBufferNeededMs.push(bufferNeededMs);
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

    this._maxBufferNeededMs.sort((a, b) => a - b);
    const targetBufferNeededMs =
      this._maxBufferNeededMs[
        Math.floor((this._maxBufferNeededMs.length * 99.5) / 100)
      ];
    const bufferSizeMs = this._bufferSizeMs;

    lc.debug?.(
      'bufferSizeMs',
      bufferSizeMs,
      'targetBufferNeededMs',
      targetBufferNeededMs,
      'this._maxBufferNeededMs.length',
      this._maxBufferNeededMs.length,
      'percentile index',
      Math.floor((this._maxBufferNeededMs.length * 99.5) / 100),
      this._maxBufferNeededMs,
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

    this._maxBufferNeededMs = [];
    this._missableCountSinceLastBufferAdjust = 0;
    this._missedCountSinceLastBufferAdjust = 0;
    this._timeOfLastBufferAdjust = now;
    this._bufferSizeMs = newBufferSizeMs;
    this._ignoreNextMissable = true;
  }

  reset() {
    this._bufferSizeMs = this._initialBufferSizeMs;
    this._maxBufferNeededMs = [];
    this._missableCountSinceLastBufferAdjust = 0;
    this._missedCountSinceLastBufferAdjust = 0;
    this._timeOfLastBufferAdjust = -1;
    this._ignoreNextMissable = false;
  }
}
