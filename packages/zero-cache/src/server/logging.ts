import {LogContext, TeeLogSink, consoleLogSink} from '@rocicorp/logger';
import {DatadogLogSink} from 'datadog/src/datadog-log-sink.js';
import {pid} from 'node:process';
import {LogConfig} from '../config/zero-config.js';

const DATADOG_SOURCE = 'zeroWorker';

function createLogSink(env: LogConfig) {
  if (env.datadogLogsApiKey === undefined) {
    return consoleLogSink;
  }
  return new TeeLogSink([
    new DatadogLogSink({
      apiKey: env.datadogLogsApiKey,
      service: env.datadogServiceLabel ?? '',
      source: DATADOG_SOURCE,
    }),
    consoleLogSink,
  ]);
}

export function createLogContext(
  config: LogConfig,
  context: {worker: string},
): LogContext {
  const ctx = {...context, pid};
  return new LogContext(config.level, ctx, createLogSink(config));
}
