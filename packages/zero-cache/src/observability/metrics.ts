import type {
  Counter,
  Histogram,
  Meter,
  MetricOptions,
  ObservableGauge,
  UpDownCounter,
} from '@opentelemetry/api';
import {metrics} from '@opentelemetry/api';

// intentional lazy initialization so it is not started before the SDK is started.

export type Category =
  | 'replication' // postgres to replica
  | 'replica' // health of replica and litestream backup
  | 'sync' // replica to client
  | 'mutation'
  | 'server';

let meter: Meter | undefined;

type Options = MetricOptions & {description: string};
type HistogramOptions = Options & {bucketBoundaries: number[]};

export const NATIVE_HISTOGRAM_INSTRUMENT_NAMES = [
  'zero.sync.view_syncer_lag',
  'zero.sync.view_syncer_hydration',
] as const;

export const LONG_DURATION_HISTOGRAM_BOUNDARIES_S = [
  1, 2, 5, 10, 30, 60, 120, 300, 600, 1200, 2400, 3600, 7200,
];

function getMeter() {
  if (!meter) {
    meter = metrics.getMeter('zero');
  }
  return meter;
}

function cache<TRet>(): (
  name: string,
  creator: (name: string) => TRet,
) => TRet {
  const instruments = new Map<string, TRet>();
  return (name: string, creator: (name: string) => TRet) => {
    const existing = instruments.get(name);
    if (existing) {
      return existing;
    }

    const ret = creator(name);
    instruments.set(name, ret);
    return ret;
  };
}

const upDownCounters = cache<UpDownCounter>();

export function getOrCreateUpDownCounter(
  category: Category,
  name: string,
  description: string,
): UpDownCounter;
export function getOrCreateUpDownCounter(
  category: Category,
  name: string,
  opts: Options,
): UpDownCounter;
export function getOrCreateUpDownCounter(
  category: Category,
  name: string,
  opts: string | Options,
): UpDownCounter {
  return upDownCounters(name, name =>
    getMeter().createUpDownCounter(
      `zero.${category}.${name}`,
      typeof opts === 'string' ? {description: opts} : opts,
    ),
  );
}

/**
 * A latency histogram whose {@link recordMs} method accepts raw millisecond
 * durations and converts them to seconds internally.
 *
 * Use {@link getOrCreateLatencyHistogram} to create one — the unit (`'s'`),
 * bucket boundaries, and ms→s conversion are all baked in
 */
export type LatencyHistogram = {
  /**
   * Record a duration. Pass the raw elapsed milliseconds — the conversion to
   * seconds (required by the `unit: 's'` OTel histogram) is handled internally.
   *
   * @param durationMs  Elapsed time in **milliseconds** (do NOT pre-divide).
   * @param attributes  Optional OTel attributes to attach to the observation.
   */
  recordMs(
    durationMs: number,
    attributes?: Parameters<Histogram['record']>[1],
  ): void;
};

/**
 * Bucket boundaries (in seconds) for zero's latency histograms.
 *
 * The operational range is 1 ms – 5,000 ms (including customers actively
 * tuning queries). ~2× logarithmic steps give proportionally consistent
 * `histogram_quantile` accuracy regardless of where values cluster within
 * that range. 10,000 ms and 30,000 ms are overflow catchers for truly broken
 * states.
 *
 *   1 ms, 2 ms, 5 ms, 10 ms, 20 ms, 50 ms, 100 ms, 200 ms, 500 ms,
 *   1 s, 2 s, 5 s, 10 s, 30 s
 */
const LATENCY_HISTOGRAM_BOUNDARIES_S = [
  0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30,
];

const histograms = cache<Histogram>();

export function getOrCreateHistogram(
  category: Category,
  name: string,
  opts: HistogramOptions,
): LatencyHistogram {
  const {bucketBoundaries, ...metricOptions} = opts;
  const h = histograms(name, name =>
    getMeter().createHistogram(`zero.${category}.${name}`, {
      ...metricOptions,
      advice: {
        ...metricOptions.advice,
        explicitBucketBoundaries: bucketBoundaries,
      },
    }),
  );
  return {
    recordMs: (durationMs, attributes) =>
      h.record(durationMs / 1000, attributes),
  };
}

// Add instruments created through this helper to
// NATIVE_HISTOGRAM_INSTRUMENT_NAMES so the SDK exports them as exponential
// histograms.
export function getOrCreateNativeHistogram(
  category: Category,
  name: string,
  opts: Options,
): LatencyHistogram {
  const h = histograms(name, name =>
    getMeter().createHistogram(`zero.${category}.${name}`, opts),
  );
  return {
    recordMs: (durationMs, attributes) =>
      h.record(durationMs / 1000, attributes),
  };
}

/**
 * Creates (or retrieves) a latency histogram for the given metric.
 *
 * - `unit` is always `'s'` (seconds), matching the OTel convention.
 * - Bucket boundaries are pre-set for zero's typical operation range
 *   (1 ms – 5 s); see {@link LATENCY_HISTOGRAM_BOUNDARIES_S}.
 * - The returned {@link LatencyHistogram} accepts **milliseconds** via
 *   `recordMs()`, so callers never need to divide by 1000.
 *
 * @example
 * ```ts
 * readonly #hydrationTime = getOrCreateLatencyHistogram(
 *   'sync', 'hydration-time', 'Time to hydrate a query.',
 * );
 * // ...
 * this.#hydrationTime.recordMs(performance.now() - start);
 * ```
 */
export function getOrCreateLatencyHistogram(
  category: Category,
  name: string,
  description: string,
): LatencyHistogram {
  return getOrCreateHistogram(category, name, {
    description,
    unit: 's',
    bucketBoundaries: LATENCY_HISTOGRAM_BOUNDARIES_S,
  });
}

const counters = cache<Counter>();

export function getOrCreateCounter(
  category: Category,
  name: string,
  description: string,
): Counter;
export function getOrCreateCounter(
  category: Category,
  name: string,
  opts: Options,
): Counter;
export function getOrCreateCounter(
  category: Category,
  name: string,
  opts: string | Options,
): Counter {
  return counters(name, name =>
    getMeter().createCounter(
      `zero.${category}.${name}`,
      typeof opts === 'string' ? {description: opts} : opts,
    ),
  );
}

const gauges = cache<ObservableGauge>();

export function getOrCreateGauge(
  category: Category,
  name: string,
  description: string,
): ObservableGauge;
export function getOrCreateGauge(
  category: Category,
  name: string,
  opts: Options,
): ObservableGauge;
export function getOrCreateGauge(
  category: Category,
  name: string,
  opts: string | Options,
): ObservableGauge {
  return gauges(name, name =>
    getMeter().createObservableGauge(
      `zero.${category}.${name}`,
      typeof opts === 'string' ? {description: opts} : opts,
    ),
  );
}
