import {
  consoleLogSink,
  TeeLogSink,
  type Context,
  type LogLevel,
  type LogSink,
} from '@rocicorp/logger';
import {DatadogLogSink, DatadogLogSinkOptions} from 'datadog';
import {version} from 'reflect-shared';

// https://www.oreilly.com/library/view/regular-expressions-cookbook/9780596802837/ch07s16.html
const IPV4_ADDRESS_REGEX =
  /^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/;
// This doesn't ensure a valid ipv6, but any ipv6 hostname will
// match this regex, and no domain based hostnames will.
const IPV6_ADDRESS_HOSTNAME_REGEX = /^\[[a-fA-F0-9:]*:[a-fA-F0-9:]*\]$/;

export const IP_ADDRESS_HOSTNAME_REGEX = new RegExp(
  `(${IPV4_ADDRESS_REGEX.source}|${IPV6_ADDRESS_HOSTNAME_REGEX.source})`,
);

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
const REFLECT_SAAS_DOMAIN = '.reflect-server.net';

export type LogOptions = {
  readonly logLevel: LogLevel;
  readonly logSink: LogSink;
};

export function createLogOptions(
  options: {
    consoleLogLevel: LogLevel;
    server: string | null;
    enableAnalytics: boolean;
  },
  createDatadogLogSink: (options: DatadogLogSinkOptions) => LogSink = (
    options: DatadogLogSinkOptions,
  ) => new DatadogLogSink(options),
): LogOptions {
  const {consoleLogLevel, server, enableAnalytics} = options;
  const socketOriginURL = server === null ? null : new URL(server);
  const socketHostname = socketOriginURL?.hostname;

  // If the hostname is undefined, localhost, or an ip address, then
  // this is most likely a test or local development, in which case we
  // do not want to send logs to datadog, instead only log to console.
  if (
    server === null ||
    socketHostname === undefined ||
    socketHostname === 'localhost' ||
    IP_ADDRESS_HOSTNAME_REGEX.test(socketHostname) ||
    !enableAnalytics
  ) {
    return {
      logLevel: consoleLogLevel,
      logSink: consoleLogSink,
    };
  }

  const datadogServiceLabel = socketHostname.endsWith(REFLECT_SAAS_DOMAIN)
    ? socketHostname
        .substring(0, socketHostname.length - REFLECT_SAAS_DOMAIN.length)
        .toLowerCase()
    : socketHostname;
  const baseURL = new URL('/api/logs/v0/log', server);
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
