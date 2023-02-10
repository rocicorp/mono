/**
 * Metrics is passed in from the app and used to record stats in
 * the client, e.g., the time to connect. A simple implementation
 * that reports to datadog can be found at
 * https://github.com/rocicorp/datadog-util.
 */
export interface Metrics {
  gauge(name: string): Gauge;
  state(name: string, clearOnFlush?: boolean | undefined): State;
}

export interface Gauge {
  set(value: number): void;
}

export interface State {
  set(value: string): void;
  clear(): void;
}

/** NopMetrics are used if no Metrics are passed in the options. */
export class NopMetrics implements Metrics {
  gauge(_name: string): Gauge {
    return nopGauge;
  }
  state(_name: string): State {
    return nopState;
  }
}

export const nopGauge: Gauge = {
  set(_value: number): void {
    // nop
  },
};

export const nopState: State = {
  set(_value: string): void {
    // nop
  },
  clear(): void {
    // nop
  },
};

export enum Metric {
  TimeToConnectMs = 'time_to_connect_ms',
  LastConnectError = 'last_connect_error',
}

// camelToSnake is used to convert a protocol ErrorKind into a suitable
// metric name, eg AuthInvalidated => auth_invalidated. It converts
// both PascalCase and camelCase to snake_case.
export function camelToSnake(s: string): string {
  return s
    .split(/\.?(?=[A-Z])/)
    .join('_')
    .toLowerCase();
}

// This value is used to indicate that the client's last connection attempt
// failed. We don't make this -1 becuase we want to stack this never connected
// state in a graph on top of actual connection times, so it should be greater
// than any other value.
export const DID_NOT_CONNECT_VALUE = 100 * 1000;
