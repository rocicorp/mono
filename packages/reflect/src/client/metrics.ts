import type {LogContext} from '@rocicorp/logger';
import type {MaybePromise} from 'replicache';

export enum MetricName {
  TimeToConnectMs = 'time_to_connect_ms',
  LastConnectError = 'last_connect_error',
}

// This value is used to indicate that the client's last connection attempt
// failed. We don't make this -1 becuase we want to stack this never connected
// state in a graph on top of actual connection times, so it should be greater
// than any other value.
export const DID_NOT_CONNECT_VALUE = 100 * 1000;

export const REPORT_INTERVAL_MS = 2 * 60 * 1000; // 2 minutes

type MetricsReporter = (metrics: Series[]) => MaybePromise<void>;

export type MetricManagerOptions = {
  reportIntervalMs: number;
  host: string;
  source: string;
  reporter: MetricsReporter;
  lc: Promise<LogContext>;
};

/**
 * MetricManager keeps track of the set of metrics in use and flushes them
 * to a format suitable for reporting.
 */
export class MetricManager {
  private _reportIntervalMs: number;
  private _host: string;
  private _reporter: MetricsReporter;
  private _lc: Promise<LogContext>;

  constructor(opts: MetricManagerOptions) {
    this._reportIntervalMs = opts.reportIntervalMs;
    this._host = opts.host;
    this._reporter = opts.reporter;
    this._lc = opts.lc;

    this.tags.push(`source:${opts.source}`);

    this.timeToConnectMs.set(DID_NOT_CONNECT_VALUE);

    setInterval(() => {
      void this.flush();
    }, this._reportIntervalMs);
  }

  private _metrics: Flushable[] = [];

  // timeToConnectMs measures the time from the call to connect() to receiving
  // the 'connected' ws message. We record the DID_NOT_CONNECT_VALUE if the previous
  // connection attempt failed for any reason.
  //
  // We set the gauge using _connectingStart as follows:
  // - _connectingStart is undefined if we are disconnected or connected; it is
  //   defined only in the Connecting state, as a number representing the timestamp
  //   at which we started connecting.
  // - _connectingStart is set to the current time when connect() is called.
  // - When we receive the 'connected' message we record the time to connect and
  //   set _connectingStart to undefined.
  // - If disconnect() is called with a defined _connectingStart then we record
  //   DID_NOT_CONNECT_VALUE and set _connectingStart to undefined.
  //
  // TODO It's clear after playing with the connection code we should encapsulate
  // the ConnectionState along with its state transitions and possibly behavior.
  // In that world the metric gauge(s) and bookkeeping like _connectingStart would
  // be encapsulated with the ConnectionState. This will probably happen as part
  // of https://github.com/rocicorp/reflect-server/issues/255.
  readonly timeToConnectMs = this._register(
    new Gauge(MetricName.TimeToConnectMs),
  );

  // lastConnectError records the last error that occurred when connecting,
  // if any. It is cleared when connecting successfully or when reported, so this
  // state only gets reported if there was a failure during the reporting period and
  // we are still not connected.
  readonly lastConnectError = this._register(
    new State(
      MetricName.LastConnectError,
      true, // clearOnFlush
    ),
  );

  /**
   * Tags to include in all metrics.
   */
  readonly tags: string[] = [];

  // Flushes all metrics to an array of time series (plural), one Series
  // per metric.
  public async flush() {
    const allSeries: Series[] = [];
    for (const metric of this._metrics) {
      const series = metric.flush();
      if (series !== undefined) {
        allSeries.push({
          ...series,
          host: this._host,
          tags: this.tags,
        });
      }
    }
    const lc = await this._lc;
    if (allSeries.length === 0) {
      lc?.debug?.('No metrics to report');
      return;
    }
    try {
      await this._reporter(allSeries);
    } catch (e) {
      lc?.error?.(`Error reporting metrics: ${e}`);
    }
  }

  private _register<M extends Flushable>(metric: M) {
    this._metrics.push(metric);
    return metric;
  }
}

// These two types are infuenced by Datadog's API's needs. We could change what
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
  private readonly _name: string;
  private _value: number | undefined = undefined;

  constructor(name: string) {
    this._name = name;
  }

  public set(value: number) {
    this._value = value;
  }

  public flush() {
    // Gauge reports the timestamp at flush time, not at the point the value was
    // recorded.
    const points =
      this._value === undefined ? [] : [makePoint(t(), this._value)];
    return {metric: this._name, points};
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
  private readonly _prefix: string;
  private readonly _clearOnFlush: boolean;
  private _current: string | undefined = undefined;

  constructor(prefix: string, clearOnFlush = false) {
    this._prefix = prefix;
    this._clearOnFlush = clearOnFlush;
  }

  public set(state: string) {
    this._current = state;
  }

  public clear() {
    this._current = undefined;
  }

  public flush() {
    if (this._current === undefined) {
      return undefined;
    }
    const gauge = new Gauge([this._prefix, this._current].join('_'));
    gauge.set(1);
    const series = gauge.flush();
    if (this._clearOnFlush) {
      this.clear();
    }
    return series;
  }
}
