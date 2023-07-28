import {consoleLogSink, LogLevel, LogSink} from '@rocicorp/logger';
import {createClientDatadogLogSink} from '@rocicorp/reflect/client';

const DEFAULT_DATADOG_SERVICE_LABEL = 'reflect.net';
const DEFAULT_LOG_LEVEL = 'info';

function getLogSinks() {
  // empty for next SSR
  if (typeof window === 'undefined') {
    return [];
  }
  if (process.env.NEXT_PUBLIC_DATADOG_LOGS_CLIENT_TOKEN === undefined) {
    console.warn(
      'Not enabling datadog logging because process.env.NEXT_PUBLIC_DATADOG_LOGS_CLIENT_TOKEN is undefined',
    );
    return [consoleLogSink];
  }
  return [
    createClientDatadogLogSink({
      clientToken: process.env.NEXT_PUBLIC_DATADOG_LOGS_CLIENT_TOKEN,
      service:
        process.env.NEXT_PUBLIC_DATADOG_SERVICE_LABEL ??
        DEFAULT_DATADOG_SERVICE_LABEL,
    }),
    consoleLogSink,
  ];
}

function getLogLevel() {
  const envLogLevel = process.env.NEXT_PUBLIC_LOG_LEVEL;
  switch (envLogLevel) {
    case 'error':
    case 'info':
    case 'debug':
      return envLogLevel;
    case undefined:
      return DEFAULT_LOG_LEVEL;
    default:
      console.log(
        'bad log level env variable value:',
        envLogLevel,
        'defaulting to:',
        DEFAULT_LOG_LEVEL,
      );
      return DEFAULT_LOG_LEVEL;
  }
}

export const loggingOptions: {logLevel: LogLevel; logSinks: LogSink[]} = {
  logLevel: getLogLevel(),
  logSinks: getLogSinks(),
};
