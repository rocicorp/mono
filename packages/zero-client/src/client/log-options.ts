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
    await consoleLogSink.flush?.();
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
    server: HTTPString | null;
    enableAnalytics: boolean;
  },
  createDatadogLogSink: (options: DatadogLogSinkOptions) => LogSink = (
    options: DatadogLogSinkOptions,
  ) => new DatadogLogSink(options),
): LogOptions {
  const {consoleLogLevel, server, enableAnalytics} = options;

  if (!enableAnalytics || server === null) {
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
  const logSink = new TeeLogSink([
    new LevelFilterLogSink(consoleLogSink, consoleLogLevel),
    new LevelFilterLogSink(
      createDatadogLogSink({
        service: datadogServiceLabel,
        host: location.host,
        version,
        baseURL,
      }),
      DATADOG_LOG_LEVEL,
    ),
  ]);
  return {
    logLevel,
    logSink,
  };
}
