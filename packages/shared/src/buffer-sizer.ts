import type {LogContext} from '@rocicorp/logger';

type OffsetStats = {
  minOffsetMs: number;
  maxOffsetMs: number;
};

export class BufferSizer {
  private _bufferSizeMs: number;
  private readonly _initialBufferSizeMs: number;
  private readonly _minBufferSizeMs: number;
  private readonly _maxBufferSizeMs: number;
  private readonly _adjustBufferSizeIntervalMs: number;
  private _offsetStats = new Map<string, OffsetStats>();
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

  recordOffset(id: string, offsetMs: number) {
    const existingStats = this._offsetStats.get(id);
    this._offsetStats.set(
      id,
      existingStats
        ? {
            minOffsetMs: Math.min(existingStats.minOffsetMs, offsetMs),
            maxOffsetMs: Math.max(existingStats.maxOffsetMs, offsetMs),
          }
        : {minOffsetMs: offsetMs, maxOffsetMs: offsetMs},
    );
  }

  recordMissable(missed: boolean) {
    if (this._ignoreNextMissable) {
      this._ignoreNextMissable = false;
      return;
    }
    this._missableCountSinceLastBufferAdjust++;
    if (missed) {
      this._missedCountSinceLastBufferAdjust++;
    }
  }

  reset() {
    this._bufferSizeMs = this._initialBufferSizeMs;
    this._offsetStats = new Map();
    this._missableCountSinceLastBufferAdjust = 0;
    this._missedCountSinceLastBufferAdjust = 0;
    this._timeOfLastBufferAdjust = -1;
    this._ignoreNextMissable = false;
  }

  maybeAdjustBufferSize(now: number, lc: LogContext): boolean {
    if (this._timeOfLastBufferAdjust === -1) {
      this._timeOfLastBufferAdjust = now;
      return false;
    }
    if (now - this._timeOfLastBufferAdjust < this._adjustBufferSizeIntervalMs) {
      return false;
    }
    if (this._missableCountSinceLastBufferAdjust === 0) {
      return false;
    }

    let maxDiffOffsetStats = undefined;
    let maxDiffID = undefined;
    for (const [id, offsetStats] of this._offsetStats) {
      if (
        maxDiffOffsetStats === undefined ||
        Math.abs(offsetStats.maxOffsetMs - offsetStats.minOffsetMs) >
          Math.abs(
            maxDiffOffsetStats.maxOffsetMs - maxDiffOffsetStats.minOffsetMs,
          )
      ) {
        maxDiffID = id;
        maxDiffOffsetStats = offsetStats;
      }
    }
    if (maxDiffOffsetStats === undefined || maxDiffID === undefined) {
      return false;
    }
    const maxDiffOffsetMs = Math.abs(
      maxDiffOffsetStats.maxOffsetMs - maxDiffOffsetStats.minOffsetMs,
    );

    const bufferSizeMs = this._bufferSizeMs;
    let newBufferSizeMs = bufferSizeMs;
    const missPercent =
      this._missedCountSinceLastBufferAdjust /
      this._missableCountSinceLastBufferAdjust;
    // This logic is pretty aggressive about adjusting up, and fairly
    // conservative about adjusting down.
    // If the miss percent is greater than 3% it will adjust up
    // to the max observed difference in offsets, or 110% of the current
    // buffer size, whichever is larger.
    // If the miss percent is less than 0.5% it will adjust down
    // to the max observer difference in offsets if its at least 10%
    // smaller than the current buffer size.
    if (missPercent > 0.03) {
      newBufferSizeMs = Math.min(
        this._maxBufferSizeMs,
        Math.max(bufferSizeMs * 1.1, maxDiffOffsetMs),
      );
      lc.debug?.(
        'Adjusting buffer up to',
        newBufferSizeMs,
        'from',
        bufferSizeMs,
        'due to high miss percent',
        missPercent,
        'over last',
        this._adjustBufferSizeIntervalMs,
        'ms',
        'based on offset stats',
        maxDiffOffsetStats,
        maxDiffOffsetMs,
        'from id',
        maxDiffID,
      );
    } else if (missPercent < 0.005) {
      const potentialNewBufferSizeMs = Math.max(
        this._minBufferSizeMs,
        Math.min(bufferSizeMs, maxDiffOffsetMs),
      );
      const percentChange =
        (potentialNewBufferSizeMs - bufferSizeMs) / bufferSizeMs;
      if (percentChange < -0.1) {
        newBufferSizeMs = potentialNewBufferSizeMs;
      }
      lc.debug?.(
        'Adjusting buffer down to',
        newBufferSizeMs,
        'from',
        bufferSizeMs,
        'due to low miss percent',
        missPercent,
        'over last',
        this._adjustBufferSizeIntervalMs,
        'ms',
        'based on offset stats',
        maxDiffOffsetStats,
        maxDiffOffsetMs,
        'from id',
        maxDiffID,
      );
    } else {
      lc.debug?.('Not adjusting buffer.');
    }

    this._offsetStats = new Map();
    this._missableCountSinceLastBufferAdjust = 0;
    this._missedCountSinceLastBufferAdjust = 0;
    this._timeOfLastBufferAdjust = now;
    this._bufferSizeMs = newBufferSizeMs;
    this._ignoreNextMissable = true;
    return this._bufferSizeMs !== bufferSizeMs;
  }
}
