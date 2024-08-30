import {LogContext, TeeLogSink, consoleLogSink} from '@rocicorp/logger';
import {DatadogLogSink} from 'datadog/src/datadog-log-sink.js';
import {pid} from 'node:process';
import {Config} from './config.js';

const DEFAULT_LOG_LEVEL = 'info';
const DATADOG_SOURCE = 'zeroWorker';

function createLogSink(
  env: Pick<Config, 'DATADOG_LOGS_API_KEY' | 'DATADOG_SERVICE_LABEL'>,
) {
  if (env.DATADOG_LOGS_API_KEY === undefined) {
    return consoleLogSink;
  }
  return new TeeLogSink([
    new DatadogLogSink({
      apiKey: env.DATADOG_LOGS_API_KEY,
      service: env.DATADOG_SERVICE_LABEL ?? '',
      source: DATADOG_SOURCE,
    }),
    consoleLogSink,
  ]);
}

function getLogLevel(env: Pick<Config, 'LOG_LEVEL'>) {
  return env.LOG_LEVEL ?? DEFAULT_LOG_LEVEL;
}

export function createLogContext(
  env: Pick<
    Config,
    'DATADOG_LOGS_API_KEY' | 'DATADOG_SERVICE_LABEL' | 'LOG_LEVEL'
  >,
  context: {worker: string},
): LogContext {
  const ctx = {...context, pid};
  return new LogContext(getLogLevel(env), ctx, createLogSink(env));
}
