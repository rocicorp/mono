import {
  consoleLogSink,
  TeeLogSink,
  type Context,
  type LogLevel,
  type LogSink,
} from '@rocicorp/logger';
import {DatadogLogSink, DatadogLogSinkOptions} from 'datadog';

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

const DATADOG_CLIENT_TOKEN = 'pub2324df3021d6fb6d6361802c3a7f6604';

const DATADOG_LOG_LEVEL = 'info';
const REFLECT_SAAS_DOMAIN = '.reflect-server.net';

export type LogOptions = {
  readonly logLevel: LogLevel;
  readonly logSink: LogSink;
};

export function createLogOptions(
  options: {
    consoleLogLevel: LogLevel;
    socketOrigin: string | null;
  },
  createDatadogLogSink: (options: DatadogLogSinkOptions) => LogSink = (
    options: DatadogLogSinkOptions,
  ) => new DatadogLogSink(options),
): LogOptions {
  const {consoleLogLevel, socketOrigin} = options;
  const socketOriginURL = socketOrigin === null ? null : new URL(socketOrigin);
  const socketHostname = socketOriginURL?.hostname;
  // If the hostname is not a subdomain of Reflect SAAS domain, then
  // this is most likely a test or local development, in which case we
  // do not want to send logs to datadog, instead only log to console.
  if (!socketHostname?.endsWith(REFLECT_SAAS_DOMAIN)) {
    return {
      logLevel: consoleLogLevel,
      logSink: consoleLogSink,
    };
  }

  const datadogServiceLabel = socketHostname
    .substring(0, socketHostname.length - REFLECT_SAAS_DOMAIN.length)
    .toLowerCase();
  const logLevel = consoleLogLevel === 'debug' ? 'debug' : 'info';
  const logSink = new TeeLogSink([
    new LevelFilterLogSink(consoleLogSink, consoleLogLevel),
    new LevelFilterLogSink(
      createDatadogLogSink({
        apiKey: DATADOG_CLIENT_TOKEN,
        service: datadogServiceLabel,
        host: location.host,
        // This has to be set to 'browser' so the server thinks we are the Datadog
        // browser SDK and we get the extra special UA/IP/GEO parsing goodness.
        source: 'browser',
      }),
      DATADOG_LOG_LEVEL,
    ),
  ]);
  return {
    logLevel,
    logSink,
  };
}
