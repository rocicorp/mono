import {REPORT_METRICS_PATH} from './server/paths.js';

export {
  createReflectServer,
  createReflectServerWithoutAuthDO,
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
export {DatadogLogSink} from './util/datadog-log-sink.js';
export {version} from './util/version.js';
export const ROUTES = {
  reportMetrics: REPORT_METRICS_PATH,
};

// TODO(arv): Only export the types that are actually used.
// https://github.com/rocicorp/reflect-server/issues/117
export * from 'replicache';
