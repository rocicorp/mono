import {
  TeeLogSink,
  consoleLogSink,
  type Context,
  type LogLevel,
  type LogSink,
} from '@rocicorp/logger';
import {
  DatadogLogSink,
  type DatadogLogSinkOptions,
} from '../../../datadog/src/datadog-log-sink.ts';
import {appendPath, type HTTPString} from './http-string.ts';
import {version} from './version.ts';

class LevelFilterLogSink implements LogSink {
  readonly #wrappedLogSink: LogSink;
  readonly #level: LogLevel;

  constructor(wrappedLogSink: LogSink, level: LogLevel) {
    this.#wrappedLogSink = wrappedLogSink;
    this.#level = level;
  }

  log(level: LogLevel, context: Context | undefined, ...args: unknown[]): void {
    if (this.#level === 'error' && level !== 'error') {
      return;
    }
    if (this.#level === 'info' && level === 'debug') {
      return;
    }
    this.#wrappedLogSink.log(level, context, ...args);
  }

  async flush() {
    await this.#wrappedLogSink.flush?.();
  }
}

const DATADOG_LOG_LEVEL = 'info';
const ZERO_SASS_DOMAIN = '.reflect-server.net';

export type LogOptions = {
  readonly logLevel: LogLevel;
  readonly logSink: LogSink;
};

export function createLogOptions(
  options: {
    consoleLogLevel: LogLevel;
    logSinks?: LogSink[] | undefined;
    server: HTTPString | null;
    enableAnalytics: boolean;
  },
  createDatadogLogSink: (options: DatadogLogSinkOptions) => LogSink = (
    options: DatadogLogSinkOptions,
  ) => new DatadogLogSink(options),
): LogOptions {
  const {consoleLogLevel, server, enableAnalytics, logSinks} = options;

  if (!enableAnalytics || server === null) {
    if (logSinks !== undefined) {
      const sink =
        logSinks.length === 1 ? logSinks[0] : new TeeLogSink(logSinks);
      return {
        logLevel: consoleLogLevel,
        logSink: sink,
      };
    }
    return {
      logLevel: consoleLogLevel,
      logSink: consoleLogSink,
    };
  }

  const serverURL = new URL(server);
  const {hostname} = serverURL;
  const datadogServiceLabel = hostname.endsWith(ZERO_SASS_DOMAIN)
    ? hostname
        .substring(0, hostname.length - ZERO_SASS_DOMAIN.length)
        .toLowerCase()
    : hostname;
  const baseURL = new URL(appendPath(server, '/logs/v0/log'));
  const logLevel = consoleLogLevel === 'debug' ? 'debug' : 'info';
  const sinks: LogSink[] =
    logSinks !== undefined
      ? [...logSinks]
      : [new LevelFilterLogSink(consoleLogSink, consoleLogLevel)];
  const datadogSink = new LevelFilterLogSink(
    createDatadogLogSink({
      service: datadogServiceLabel,
      host: location.host,
      version,
      baseURL,
    }),
    DATADOG_LOG_LEVEL,
  );
  sinks.push(datadogSink);
  const logSink = sinks.length === 1 ? sinks[0] : new TeeLogSink(sinks);
  return {
    logLevel,
    logSink,
  };
}
