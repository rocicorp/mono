import {REPORT_METRICS_PATH} from './server/paths.js';
import {DatadogLogSink} from 'datadog';

export {
  createReflectServer,
  ReflectServerOptions,
  ReflectServerBaseEnv,
} from './server/reflect.js';
export type {AuthHandler, UserData} from './server/auth.js';
export type {DisconnectHandler} from './server/disconnect.js';
export {
  consoleLogSink,
  TeeLogSink,
  type LogSink,
  type LogLevel,
} from '@rocicorp/logger';
export {version} from './util/version.js';
export const ROUTES = {
  reportMetrics: REPORT_METRICS_PATH,
};
export {createDatadogMetricsSink} from './server/datadog-metrics-sink.js';

export type WorkerDatadogLogSinkOptions = {
  apiKey: string;
  service?: string | undefined;
  host?: string | undefined;
};
export function createWorkerDatadogLogSink(opts: WorkerDatadogLogSinkOptions) {
  return new DatadogLogSink({...opts, source: 'worker'});
}

// TODO(arv): Only export the types that are actually used.
// https://github.com/rocicorp/reflect-server/issues/117
export * from 'replicache';
