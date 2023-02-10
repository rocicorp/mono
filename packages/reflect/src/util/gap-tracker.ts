import {TDigest} from 'tdigest';
import type {LogContext} from '@rocicorp/logger';

export class GapTracker {
  private _name: string;
  private _summaryFrequencyMs: number;
  private _lastSummaryMs: number | undefined;
  private _last: number | undefined;
  private _lc: LogContext;
  private readonly _digest: TDigest;

  constructor(name: string, lc: LogContext, summaryFrequencyMs = 1000) {
    this._name = name;
    this._lc = lc;
    this._summaryFrequencyMs = summaryFrequencyMs;
    this._digest = new TDigest();
  }

  push(value: number) {
    if (this._last !== undefined) {
      const diff = value - this._last;
      this._digest.push(diff);
    }
    this._last = value;

    const now = performance.now();
    if (this._lastSummaryMs === undefined) {
      this._lastSummaryMs = now;
    } else if (now - this._lastSummaryMs > this._summaryFrequencyMs) {
      this._lc.debug?.(`${this._name} gap summary`, this._digest.summary());
      this._lastSummaryMs = now;
    }
  }
}
