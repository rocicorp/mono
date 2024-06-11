import {TeeLogSink, consoleLogSink} from '@rocicorp/logger';
import {DatadogLogSink} from 'datadog/src/datadog-log-sink.js';
import type {ServiceRunnerEnv} from './service-runner.js';

const DEFAULT_LOG_LEVEL = 'info';
const DATADOG_SOURCE = 'zeroWorker';

export function createLogSink(env: ServiceRunnerEnv) {
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

export function getLogLevel(env: ServiceRunnerEnv) {
  return env.LOG_LEVEL ?? DEFAULT_LOG_LEVEL;
}
