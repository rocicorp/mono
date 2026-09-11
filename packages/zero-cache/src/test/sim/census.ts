import type {Attributes, Context, Counter} from '@opentelemetry/api';
import type * as Metrics from '../../observability/metrics.ts';
import {currentIncarnation} from './incarnation.ts';
import type {TraceEvent} from './trace.ts';

/**
 * Outcomes a sweep must reach, counted from what the code under test reports:
 * its counters where it has one, its log lines otherwise. A sweep that never
 * reaches an expected outcome fails, so generator weights that drift away from
 * a path are noticed rather than trusted.
 */
export class Census {
  readonly #counts = new Map<string, number>();
  readonly #alarms: string[] = [];

  note(outcome: string, n = 1): void {
    this.#counts.set(outcome, (this.#counts.get(outcome) ?? 0) + n);
  }

  count(outcome: string): number {
    return this.#counts.get(outcome) ?? 0;
  }

  merge(other: Census): void {
    for (const [outcome, n] of other.#counts) {
      this.note(outcome, n);
    }
  }

  /** The `expected` outcomes that never happened. */
  missing(expected: readonly string[]): string[] {
    return expected.filter(outcome => this.count(outcome) === 0);
  }

  entries(): [string, number][] {
    const entries = [...this.#counts];
    entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return entries;
  }

  /**
   * Alarms raised since the last call: log lines by which the code under test
   * reports a broken invariant of its own. A fenced incarnation's are ignored,
   * since its failures are the crash's doing.
   */
  takeAlarms(): string[] {
    return this.#alarms.splice(0);
  }

  /** Counts outcomes, and alarms, that only a log line reports. */
  observe(event: TraceEvent): void {
    if (!event.kind.startsWith('log.')) {
      return;
    }
    const {args} = event.data as {args: unknown[]};
    const [message, detail] = args;
    if (typeof message !== 'string') {
      return;
    }
    for (const [pattern, outcome] of LOG_OUTCOMES) {
      if (message.includes(pattern)) {
        this.note(typeof outcome === 'string' ? outcome : outcome(detail));
      }
    }
    if (
      (event.kind === 'log.error' || event.kind === 'log.warn') &&
      ALARMS.some(alarm => message.includes(alarm)) &&
      !currentIncarnation()?.fenced
    ) {
      this.#alarms.push(
        `${event.node}#${event.inc}: ${message}` +
          (detail instanceof Error ? ` ${String(detail)}` : ''),
      );
    }
  }
}

/** Log lines that report a broken invariant, or a harness bug. */
const ALARMS = [
  // The purge scheduler's invariant-14 probe.
  'purged history at or above the purge floor',
  // A truncate-above that did not happen.
  'violated a constraint',
  // A replicator failed to apply the stream.
  'Message Processing failed',
  // A lock wait: fresh paths per incarnation should leave none.
  'SQLITE_BUSY for',
  // A purge pass that failed, in an incarnation that was not fenced.
  'error purging the SQLite change log',
];

/** The reasons with which the backfill manager stops a run at DDL. */
const DDL_CANCEL_REASON = /^(?:table|column) (?:renamed|dropped)$/;

const LOG_OUTCOMES: [string, string | ((detail: unknown) => string)][] = [
  ['finished backfilling', 'backfill:completed'],
  [
    'canceling backfill:',
    detail =>
      DDL_CANCEL_REASON.test(String(detail))
        ? 'backfill:canceled/ddl'
        : 'backfill:canceled/other',
  ],
  [
    'reseeded the SQLite change log',
    detail =>
      `reconcile:reseeded/${
        (detail as {sqliteChangeLogReconcile?: {reason?: string}} | undefined)
          ?.sqliteChangeLogReconcile?.reason
      }`,
  ],
  ['SQLite change log is at the resume watermark', 'reconcile:keep'],
  ['truncated phantom transactions', 'reconcile:truncated'],
  ['timed out waiting for /sync result', 'checkpointer:soft-wait-timeout'],
  [
    'waiting for litestream to successfully checkpoint',
    'checkpointer:hard-pause',
  ],
  ['releasing the snapshot reservation for', 'reservation:expired'],
];

/** Counters whose increments the census counts, by instrument name. */
const COUNTER_OUTCOMES: Record<string, (attrs: Attributes) => string> = {
  'sqlite_change_log.catchup_routes': a =>
    `route:${a['source']}/${a['reason']}`,
  'sqlite_change_log.reservation_invalidations': a =>
    `reservation:invalidated/${a['reason']}`,
  'sqlite_change_log.reservation_confirm_delays': () =>
    'reservation:confirm-delayed',
  'flow_control.waits': a => `flow-control:wait/${a['release.mode']}`,
  'sqlite_change_log.purge_declined': a => `purge:stopped/${a['reason']}`,
  'sqlite_change_log.purge_floor_probe': a => `purge:probe/${a['outcome']}`,
  'sqlite_change_log.barrier_timeouts': () => 'barrier:timeout',
  'backfill_runs': a => `backfill:run/${a['start']}`,
  'backfill_restarts': a => `backfill:restart/${a['reason']}`,
  'backfill_reannouncements': () => 'backfill:reannounced',
  'backfill_declarations': a => `declaration:${a['outcome']}`,
};

let active: Census | undefined;

/** Counts counter increments into `census`, until set again. */
export function setActiveCensus(census: Census | undefined): void {
  active = census;
}

/**
 * Wraps the metrics module for a simulation test file, so that the census can
 * tell counters apart:
 *
 * ```ts
 * vi.mock(import('../../observability/metrics.ts'), async importOriginal => {
 *   const {observeMetrics} = await import('./census.ts');
 *   return observeMetrics(await importOriginal());
 * });
 * ```
 *
 * With no meter provider installed, every instrument is one shared no-op
 * object, so nothing after the fact can say which counter was incremented.
 */
export function observeMetrics(mod: typeof Metrics): typeof Metrics {
  const getOrCreateCounter = (
    category: Parameters<typeof mod.getOrCreateCounter>[0],
    name: string,
    opts: string,
  ): Counter => {
    const counter = mod.getOrCreateCounter(category, name, opts);
    const outcome = COUNTER_OUTCOMES[name];
    if (!outcome) {
      return counter;
    }
    return {
      add(value: number, attributes?: Attributes, context?: Context) {
        counter.add(value, attributes, context);
        active?.note(outcome(attributes ?? {}), value);
      },
    };
  };
  return {
    ...mod,
    getOrCreateCounter: getOrCreateCounter as typeof mod.getOrCreateCounter,
  };
}
