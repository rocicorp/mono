import {getOrCreateLatencyHistogram} from '../../observability/metrics.ts';
import type {ResetPipelinesReason} from './snapshotter.ts';

export type AdvancementOutcome =
  | {readonly outcome: 'success' | 'error'}
  | {readonly outcome: 'reset'; readonly reason: ResetPipelinesReason};

export type AdvancementTimings = {
  /** Setup plus time between time-slice yields, including asynchronous I/O. */
  readonly processingTimeMs: number;
  /** Synchronous snapshot, change-count, CVR updater, and poke preparation. */
  readonly setupTimeMs: number;
  /** Elapsed wall time, including time-slice waits and output cleanup. */
  readonly wallTimeMs: number;
};

/**
 * Counts and times every advancement attempt, including work abandoned by a
 * reset. The existing sync.advance-time histogram measures successes only.
 * Outcome and reset reason are bounded labels; client and query IDs are not
 * recorded here. Recording once per attempt adds no clocks to row processing.
 */
export class AdvancementMetrics {
  readonly #processingTime = getOrCreateLatencyHistogram(
    'sync',
    'advance-attempt-time',
    'Processing time for an advancement attempt, including setup and ' +
      'asynchronous I/O. Recorded for successful, reset, and failed attempts; ' +
      'excludes only time-slice waits.',
  );
  readonly #setupTime = getOrCreateLatencyHistogram(
    'sync',
    'advance-attempt-setup-time',
    'Synchronous setup time for an advancement attempt: snapshot acquisition, ' +
      'change counting, CVR updater construction, and poke preparation.',
  );
  readonly #wallTime = getOrCreateLatencyHistogram(
    'sync',
    'advance-attempt-wall-time',
    'Elapsed wall time for an advancement attempt, including time-slice ' +
      'waits, CVR flush, and output cleanup.',
  );

  record(
    {processingTimeMs, setupTimeMs, wallTimeMs}: AdvancementTimings,
    outcome: AdvancementOutcome,
  ): void {
    const attributes = {
      outcome: outcome.outcome,
      ...(outcome.outcome === 'reset' && {reset_reason: outcome.reason}),
    };
    this.#processingTime.recordMs(processingTimeMs, attributes);
    this.#setupTime.recordMs(setupTimeMs, attributes);
    this.#wallTime.recordMs(wallTimeMs, attributes);
  }
}
