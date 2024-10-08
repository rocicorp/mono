import {LogContext, TeeLogSink, consoleLogSink} from '@rocicorp/logger';
import {DatadogLogSink} from '../../../datadog/src/mod.js';
import {pid} from 'node:process';
import {type LogConfig} from '../config/zero-config.js';

const DATADOG_SOURCE = 'zeroWorker';

function createLogSink(config: LogConfig) {
  if (config.datadogLogsApiKey === undefined) {
    return consoleLogSink;
  }
  return new TeeLogSink([
    new DatadogLogSink({
      apiKey: config.datadogLogsApiKey,
      service: config.datadogServiceLabel ?? '',
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
