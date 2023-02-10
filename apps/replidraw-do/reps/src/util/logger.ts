/**
 * A logger interface exposing optional [[error]], [[info]] and [[debug]]
 * methods. The idea is that these gets filled in based on the [[LogLevel]]. If
 * [[LogLevel]] is 'debug' then all three should be present, if it is 'info',
 * then [[error]] and [[info]] should be present and if [[LogLevel]] is 'error'
 * only [[error]] will be present.
 */
export interface Logger {
  error?(...args: unknown[]): void;
  info?(...args: unknown[]): void;
  debug?(...args: unknown[]): void;
}

/**
 * The different log levels. This is used to determine how much logging to do.
 * `'error'` > `'info'` > `'debug'`... meaning `'error'` has highest priority
 * and `'debug'` lowest.
 */
export type LogLevel = "error" | "info" | "debug";

export function getLogger(prefix: string[], level: LogLevel): Logger {
  const logger: Logger = {};
  const impl =
    (name: LogLevel) =>
    (...args: unknown[]) =>
      console[name](...prefix, ...args);
  /* eslint-disable no-fallthrough , @typescript-eslint/ban-ts-comment */
  switch (level) {
    // @ts-ignore
    case "debug":
      logger.debug = impl("debug");
    // @ts-ignore
    case "info":
      logger.info = impl("info");
    case "error":
      logger.error = impl("error");
  }
  /* eslint-ensable @typescript-eslint/ban-ts-comment, no-fallthrough */

  return logger;
}

export class LogContext implements Logger {
  private readonly _s;

  readonly debug?: (...args: unknown[]) => void = undefined;
  readonly info?: (...args: unknown[]) => void = undefined;
  readonly error?: (...args: unknown[]) => void = undefined;

  constructor(level: LogLevel = "info", s = "") {
    this._s = s;

    const impl =
      (name: LogLevel) =>
      (...args: unknown[]) =>
        console[name](this._s, ...args);

    /* eslint-disable no-fallthrough , @typescript-eslint/ban-ts-comment */
    switch (level) {
      // @ts-ignore
      case "debug":
        this.debug = impl("debug");
      // @ts-ignore
      case "info":
        this.info = impl("info");
      case "error":
        this.error = impl("error");
    }
    /* eslint-ensable @typescript-eslint/ban-ts-comment, no-fallthrough */
  }

  addContext(key: string, value: unknown): LogContext {
    const space = this._s ? " " : "";
    return new LogContext(this._logLevel, `${this._s}${space}${key}=${value}`);
  }

  private get _logLevel(): LogLevel {
    return this.debug ? "debug" : this.info ? "info" : "error";
  }
}
