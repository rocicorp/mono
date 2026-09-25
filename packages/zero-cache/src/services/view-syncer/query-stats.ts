import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import type {PlanWarning} from '../../../../zql/src/planner/planner-warnings.ts';
import type {QueryShape} from './query-shape.ts';

/** The query that work is accounted to. */
export type QueryIdentity = {
  readonly queryName?: string | undefined;
  readonly shape: QueryShape;
};

export type HydrationOutcome = 'finished' | 'aborted' | 'failed';

export type HydrationRecord = {
  readonly outcome: HydrationOutcome;
  readonly timeMs: number;
  /** Only for a finished hydration. */
  readonly rowCount?: number | undefined;
  /** Only for a finished hydration. */
  readonly rowsRead?: number | undefined;
  readonly planWarnings?: readonly PlanWarning[] | undefined;
};

export type AdvanceRecord = {
  /** The time spent processing the pushes to the query. */
  readonly timeMs: number;
  /** The number of changes pushed to the query. */
  readonly changes: number;
  /** Whether the advancement timed out and reset the pipelines. */
  readonly timedOut: boolean;
};

export type QueryStatsOptions = {
  /**
   * The most query shapes tracked per interval. Work on shapes beyond this is
   * accounted to the interval's totals only, which bounds memory when the
   * shape space is unbounded (e.g. dynamically built queries).
   */
  readonly maxShapes?: number | undefined;

  /**
   * The most query shapes logged per interval, those that took the most
   * time. The rest are included in the interval's totals.
   */
  readonly maxReported?: number | undefined;

  readonly now?: (() => number) | undefined;
};

class Distribution {
  count = 0;
  sumMs = 0;
  minMs = Infinity;
  maxMs = 0;

  add(ms: number) {
    this.count++;
    this.sumMs += ms;
    this.minMs = Math.min(this.minMs, ms);
    this.maxMs = Math.max(this.maxMs, ms);
  }

  toJSON() {
    return {
      count: this.count,
      sumMs: round(this.sumMs),
      minMs: round(this.count === 0 ? 0 : this.minMs),
      maxMs: round(this.maxMs),
    };
  }
}

class Totals {
  readonly hydrations = new Distribution();
  hydrationsAborted = 0;
  hydrationsFailed = 0;
  hydrationRowCount = 0;
  hydrationRowsRead = 0;
  readonly advances = new Distribution();
  advanceChanges = 0;
  advanceTimeouts = 0;

  get timeMs() {
    return this.hydrations.sumMs + this.advances.sumMs;
  }

  addHydration({outcome, timeMs, rowCount, rowsRead}: HydrationRecord) {
    this.hydrations.add(timeMs);
    if (outcome === 'aborted') {
      this.hydrationsAborted++;
    } else if (outcome === 'failed') {
      this.hydrationsFailed++;
    }
    this.hydrationRowCount += rowCount ?? 0;
    this.hydrationRowsRead += rowsRead ?? 0;
  }

  addAdvance({timeMs, changes, timedOut}: AdvanceRecord) {
    this.advances.add(timeMs);
    this.advanceChanges += changes;
    if (timedOut) {
      this.advanceTimeouts++;
    }
  }

  toJSON() {
    return {
      timeMs: round(this.timeMs),
      hydrations: this.hydrations.toJSON(),
      hydrationsAborted: this.hydrationsAborted,
      hydrationsFailed: this.hydrationsFailed,
      hydrationRowCount: this.hydrationRowCount,
      hydrationRowsRead: this.hydrationRowsRead,
      advances: this.advances.toJSON(),
      advanceChanges: this.advanceChanges,
      advanceTimeouts: this.advanceTimeouts,
    };
  }
}

class ShapeStats extends Totals {
  readonly query: QueryIdentity;
  planWarnings: readonly PlanWarning[] | undefined;

  constructor(query: QueryIdentity) {
    super();
    this.query = query;
  }
}

/** A fixed-size linear-counting sketch for shapes beyond the tracking limit. */
class OverflowShapeCounter {
  readonly #buckets = new Uint8Array(4096);
  #occupied = 0;

  add(key: string): void {
    // FNV-1a gives a stable bucket without retaining the key.
    let hash = 0x811c9dc5;
    for (let i = 0; i < key.length; i++) {
      hash = Math.imul(hash ^ key.charCodeAt(i), 0x01000193);
    }
    const bucket = (hash >>> 0) % this.#buckets.length;
    if (this.#buckets[bucket] === 0) {
      this.#buckets[bucket] = 1;
      this.#occupied++;
    }
  }

  get estimatedCount(): number {
    const empty = this.#buckets.length - this.#occupied;
    return empty === 0
      ? this.#buckets.length
      : Math.round(
          -this.#buckets.length * Math.log(empty / this.#buckets.length),
        );
  }
}

/**
 * Aggregates the work done for each query shape (the query with its values
 * redacted, see {@link QueryShape}) on a worker, and periodically logs it,
 * like `pg_stat_statements` does for Postgres.
 *
 * Unlike the slow query logs, which are throttled and only written above a
 * threshold, every hydration and advancement is counted, so the logs can be
 * summed over any time range and across workers to rank queries by the time
 * they take.
 *
 * Each {@link flush} logs the interval since the previous one:
 *
 * - a `query-stats` event per query shape, for the {@link
 *   QueryStatsOptions.maxReported} shapes that took the most time, and
 * - a `query-stats-summary` event with the totals of all shapes, including
 *   those not reported individually.
 *
 * The events carry counts, sums, minimums and maximums, which compose across
 * intervals and workers.
 */
export class QueryStats {
  readonly #maxShapes: number;
  readonly #maxReported: number;
  readonly #now: () => number;

  #shapes = new Map<string, ShapeStats>();
  #totals = new Totals();
  #overflowShapes = new OverflowShapeCounter();
  #intervalStart: number;

  constructor({
    maxShapes = 1000,
    maxReported = 100,
    now = Date.now,
  }: QueryStatsOptions = {}) {
    assert(maxShapes > 0, 'maxShapes must be positive');
    assert(maxReported > 0, 'maxReported must be positive');
    this.#maxShapes = maxShapes;
    this.#maxReported = maxReported;
    this.#now = now;
    this.#intervalStart = now();
  }

  recordHydration(query: QueryIdentity, record: HydrationRecord): void {
    this.#totals.addHydration(record);
    const stats = this.#stats(query);
    if (stats) {
      stats.addHydration(record);
      if (record.planWarnings !== undefined) {
        stats.planWarnings = record.planWarnings;
      }
    }
  }

  recordAdvance(query: QueryIdentity, record: AdvanceRecord): void {
    this.#totals.addAdvance(record);
    this.#stats(query)?.addAdvance(record);
  }

  #stats(query: QueryIdentity): ShapeStats | undefined {
    const key = `${query.queryName ?? ''}:${query.shape.hash}`;
    let stats = this.#shapes.get(key);
    if (stats === undefined) {
      if (this.#shapes.size >= this.#maxShapes) {
        this.#overflowShapes.add(key);
        return undefined;
      }
      stats = new ShapeStats({queryName: query.queryName, shape: query.shape});
      this.#shapes.set(key, stats);
    }
    return stats;
  }

  /**
   * Logs the stats of the interval since the previous flush, and starts a
   * new interval. Nothing is logged for an interval without any work.
   */
  flush(lc: LogContext): void {
    const now = this.#now();
    const intervalMs = now - this.#intervalStart;
    const shapes = this.#shapes;
    const totals = this.#totals;
    const overflowShapes = this.#overflowShapes.estimatedCount;
    this.#shapes = new Map();
    this.#totals = new Totals();
    this.#overflowShapes = new OverflowShapeCounter();
    this.#intervalStart = now;

    if (shapes.size === 0 && overflowShapes === 0) {
      return;
    }
    const reported = [...shapes.values()]
      .toSorted((a, b) => b.timeMs - a.timeMs)
      .slice(0, this.#maxReported);
    for (const stats of reported) {
      const {queryName, shape} = stats.query;
      lc.info?.('query stats', {
        zeroEvent: 'query-stats',
        intervalMs,
        ...(queryName !== undefined && {queryName}),
        queryShape: shape.hash,
        ...stats.toJSON(),
        ...(stats.planWarnings !== undefined &&
          stats.planWarnings.length > 0 && {planWarnings: stats.planWarnings}),
        zql: shape.zql,
      });
    }
    lc.info?.('query stats summary', {
      zeroEvent: 'query-stats-summary',
      intervalMs,
      shapes: shapes.size + overflowShapes,
      shapesReported: reported.length,
      ...totals.toJSON(),
    });
  }

  /**
   * Flushes every `intervalMs` until the returned function is called, which
   * flushes one last time.
   */
  start(lc: LogContext, intervalMs: number): () => void {
    const timer = setInterval(() => this.flush(lc), intervalMs);
    timer.unref?.();
    return () => {
      clearInterval(timer);
      this.flush(lc);
    };
  }
}

function round(ms: number) {
  return Math.round(ms * 100) / 100;
}
