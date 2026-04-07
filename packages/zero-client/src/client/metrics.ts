import type {LogContext} from '@rocicorp/logger';
import type {MaybePromise} from '../../../shared/src/types.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {
  isClientError,
  isServerError,
  type ZeroError,
  type ZeroErrorKind,
} from './error.ts';
import {MetricName} from './metric-name.ts';

// This value is used to indicate that the client's last connection attempt
// failed. We don't make this -1 because we want to stack this never connected
// state in a graph on top of actual connection times, so it should be greater
// than any other value.
export const DID_NOT_CONNECT_VALUE = 100 * 1000;

export const REPORT_INTERVAL_MS = 5_000;

type NotConnectedReason =
  | 'init'
  | 'error'
  | 'hidden'
  | 'hidden_was_init'
  | 'hidden_was_error';

export function getLastConnectErrorValue(reason: ZeroError) {
  return `${isServerError(reason) ? 'server_' : 'client_'}${camelToSnake(reason.kind)}` as const;
}

// camelToSnake is used to convert a ZeroErrorKind into a suitable
// metric name, eg AuthInvalidated => auth_invalidated. It converts
// both PascalCase and camelCase to snake_case.
function camelToSnake(kind: ZeroErrorKind): string {
  return kind
    .split(/\.?(?=[A-Z])/)
    .join('_')
    .toLowerCase();
}

/**
 * Returns whether an error should be reported in metrics and
 * increment the connect error count.
 *
 * Returns `true` for all server errors and client errors that represent actual
 * connection problems. Returns `false` for expected client-side disconnections
 * (user disconnect, client closed, hidden tab, clean/abrupt close).
 */
export function shouldReportConnectError(reason: ZeroError): boolean {
  if (!isClientError(reason)) {
    return true;
  }
  switch (reason.kind) {
    case ClientErrorKind.Hidden:
    case ClientErrorKind.ClientClosed:
    case ClientErrorKind.UserDisconnect:
    case ClientErrorKind.CleanClose:
    case ClientErrorKind.AbruptClose:
      return false;
    default:
      return true;
  }
}

type MetricsReporter = (metrics: Series[]) => MaybePromise<void>;

export type MetricManagerOptions = {
  reportIntervalMs: number;
  host: string;
  source: string;
  reporter: MetricsReporter;
  lc: LogContext;
};

/**
 * MetricManager keeps track of the set of metrics in use and flushes them
 * to a format suitable for reporting.
 */
export class MetricManager {
  #reportIntervalMs: number;
  #host: string;
  #reporter: MetricsReporter;
  #lc: LogContext;
  #timerID: ReturnType<typeof setInterval> | null;

  constructor(opts: MetricManagerOptions) {
    this.#reportIntervalMs = opts.reportIntervalMs;
    this.#host = opts.host;
    this.#reporter = opts.reporter;
    this.#lc = opts.lc;

    this.tags.push(`source:${opts.source}`);

    this.timeToConnectMs.set(DID_NOT_CONNECT_VALUE);
    this.#setNotConnectedReason('init');

    this.#timerID = setInterval(() => {
      void this.flush();
    }, this.#reportIntervalMs);
  }

  #metrics: Flushable[] = [];

  // timeToConnectMs measures the time from the call to connect() to receiving
  // the 'connected' ws message. We record the DID_NOT_CONNECT_VALUE if the previous
  // connection attempt failed for any reason.
  //
  // We set the gauge using #connectStart as follows:
  // - #connectStart is undefined if we are disconnected or connected; it is
  //   defined only in the Connecting state, as a number representing the timestamp
  //   at which we started connecting.
  // - #connectStart is set to the current time when connect() is called.
  // - When we receive the 'connected' message we record the time to connect and
  //   set #connectStart to undefined.
  // - If disconnect() is called with a defined #connectStart then we record
  //   DID_NOT_CONNECT_VALUE and set #connectStart to undefined.
  //
  // TODO: this should be folded into the ConnectionManager.
  readonly timeToConnectMs = this.#register(
    new Gauge(MetricName.TimeToConnectMs),
  );

  // lastConnectError records the last error that occurred when connecting,
  // if any. It is cleared when connecting successfully or when reported, so this
  // state only gets reported if there was a failure during the reporting period and
  // we are still not connected.
  readonly lastConnectError = this.#register(
    new State(
      MetricName.LastConnectError,
      true, // clearOnFlush
    ),
  );

  // notConnected records the reason why the client is not currently connected.
  // It is cleared when the client successfully connects.
  readonly #notConnected = this.#register(new State(MetricName.NotConnected));

  // The time from the call to connect() to receiving the 'connected' ws message
  // for the current connection.  Cleared when the client is not connected.
  // TODO: Not actually currently cleared on disconnect untill there is a
  // connect error, or client reports disconnected and waiting for visible.
  // Should have a value iff _notConnected has no value.
  readonly #timeToConnectMsV2 = this.#register(
    new Gauge(MetricName.TimeToConnectMsV2),
  );

  // lastConnectErrorV2 records the last error that occurred when connecting,
  // if any. It is cleared when the client successfully connects or
  // stops trying to connect due to being hidden.
  // Should have a value iff notConnected state is NotConnectedReason.Error.
  readonly #lastConnectErrorV2 = this.#register(
    new State(MetricName.LastConnectErrorV2),
  );

  // The total time it took to connect across retries for the current
  // connection.  Cleared when the client is not connected.
  // TODO: Not actually currently cleared on disconnect until there is a
  // connect error, or client reports disconnected and waiting for visible.
  // See Zero.#totalToConnectStart for details of how this total is computed.
  // Should have a value iff _notConnected has no value.
  readonly #totalTimeToConnectMs = this.#register(
    new Gauge(MetricName.TotalTimeToConnectMs),
  );

  #setNotConnectedReason(reason: NotConnectedReason) {
    this.#notConnected.set(reason);
  }

  setConnected(timeToConnectMs: number, totalTimeToConnectMs: number) {
    this.#notConnected.clear();
    this.#lastConnectErrorV2.clear();
    this.#timeToConnectMsV2.set(timeToConnectMs);
    this.#totalTimeToConnectMs.set(totalTimeToConnectMs);
  }

  setDisconnectedWaitingForVisible() {
    this.#timeToConnectMsV2.clear();
    this.#totalTimeToConnectMs.clear();
    this.#lastConnectErrorV2.clear();
    let notConnectedReason: NotConnectedReason;
    switch (this.#notConnected.get()) {
      case 'init':
        notConnectedReason = 'hidden_was_init';
        break;
      case 'error':
        notConnectedReason = 'hidden_was_error';
        break;
      default:
        notConnectedReason = 'hidden';
        break;
    }
    this.#setNotConnectedReason(notConnectedReason);
  }

  setConnectError(reason: ZeroError) {
    this.#timeToConnectMsV2.clear();
    this.#totalTimeToConnectMs.clear();
    this.#setNotConnectedReason('error');
    this.#lastConnectErrorV2.set(getLastConnectErrorValue(reason));
  }

  /**
   * Tags to include in all metrics.
   */
  readonly tags: string[] = [];

  // Flushes all metrics to an array of time series (plural), one Series
  // per metric.
  async flush() {
    const lc = this.#lc;
    if (this.#timerID === null) {
      lc.error?.('MetricManager.flush() called but already stopped');
      return;
    }
    const allSeries: Series[] = [];
    for (const metric of this.#metrics) {
      const series = metric.flush();
      if (series !== undefined) {
        allSeries.push({
          ...series,
          host: this.#host,
          tags: this.tags,
        });
      }
    }
    if (allSeries.length === 0) {
      lc?.debug?.('No metrics to report');
      return;
    }
    try {
      await this.#reporter(allSeries);
    } catch (e) {
      lc?.error?.('Error reporting metrics', e);
    }
  }

  stop() {
    if (this.#timerID === null) {
      this.#lc.error?.('MetricManager.stop() called but already stopped');
      return;
    }
    clearInterval(this.#timerID);
    this.#timerID = null;
  }

  #register<M extends Flushable>(metric: M) {
    this.#metrics.push(metric);
    return metric;
  }
}

// These two types are influenced by Datadog's API's needs. We could change what
// we use internally if necessary, but we'd just have to convert to/from before
// sending to DD. So for convenience we go with their format.

/** Series is a time series of points for a single metric. */
export type Series = {
  host: string;
  metric: string; // We call this 'name' bc 'metric' is overloaded in code.
  points: Point[];
  tags?: string[];
};
/**
 * A point is a second-resolution timestamp and a set of values for that
 * timestamp. A point represents exactly one second in time and the values
 * are those recorded for that second. The first element of this array
 * is the timestamp and the second element is an array of values.
 */
export type Point = [number, number[]];

function makePoint(ts: number, value: number): Point {
  return [ts, [value]];
}

type Flushable = {
  flush(): Pick<Series, 'metric' | 'points'> | undefined;
};

/**
 * Gauge is a metric type that represents a single value that can go up and
 * down. It's typically used to track discrete values or counts eg the number
 * of active users, number of connections, cpu load, etc. A gauge retains
 * its value when flushed.
 *
 * We use a Gauge to sample at the client. If we are interested in tracking
 * a metric value *per client*, the client can note the latest value in
 * a Gauge metric. The metric is periodically reported via Reporter. On the
 * server, we graph the value of the metric rolled up over the periodic
 * reporting period, that is, counted over a span of time equal to the
 * reporting period. The result is ~one point per client per reporting
 * period.
 */
export class Gauge implements Flushable {
  readonly #name: string;
  #value: number | undefined = undefined;

  constructor(name: string) {
    this.#name = name;
  }

  set(value: number) {
    this.#value = value;
  }

  get() {
    return this.#value;
  }

  clear() {
    this.#value = undefined;
  }

  flush() {
    if (this.#value === undefined) {
      return undefined;
    }
    // Gauge reports the timestamp at flush time, not at the point the value was
    // recorded.
    const points = [makePoint(t(), this.#value)];
    return {metric: this.#name, points};
  }
}

function t() {
  return Math.round(Date.now() / 1000);
}

/**
 * State is a metric type that represents a specific state that the system is
 * in, for example the state of a connection which may be 'open' or 'closed'.
 * The state is given a name/prefix at construction time (eg 'connection') and
 * then can be set to a specific state (eg 'open'). The prefix is prepended to
 * the set state (eg, 'connection_open') and a value of 1 is reported.
 * Unset/cleared states are not reported.
 *
 * Example:
 *   const s = new State('connection');
 *   s.set('open');
 *   s.flush(); // returns {metric: 'connection_open', points: [[now(), [1]]]}
 */
export class State implements Flushable {
  readonly #prefix: string;
  readonly #clearOnFlush: boolean;
  #current: string | undefined = undefined;

  constructor(prefix: string, clearOnFlush = false) {
    this.#prefix = prefix;
    this.#clearOnFlush = clearOnFlush;
  }

  set(state: string) {
    this.#current = state;
  }

  get() {
    return this.#current;
  }

  clear() {
    this.#current = undefined;
  }

  flush() {
    if (this.#current === undefined) {
      return undefined;
    }
    const gauge = new Gauge([this.#prefix, this.#current].join('_'));
    gauge.set(1);
    const series = gauge.flush();
    if (this.#clearOnFlush) {
      this.clear();
    }
    return series;
  }
}
