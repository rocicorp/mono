import {DatadogLogSink} from 'datadog/src/mod.js';
import {REPORT_METRICS_PATH} from './server/paths.js';

export {
  TeeLogSink,
  consoleLogSink,
  type LogLevel,
  type LogSink,
} from '@rocicorp/logger';
export type {AuthHandler} from './server/auth.js';
export type {DisconnectHandler} from './server/disconnect.js';
export {
  ReflectServerBaseEnv,
  ReflectServerOptions,
  createReflectServer,
} from './server/reflect.js';
export const ROUTES = {
  reportMetrics: REPORT_METRICS_PATH,
};
export type WorkerDatadogLogSinkOptions = {
  apiKey: string;
  service?: string | undefined;
  host?: string | undefined;
};
export function createWorkerDatadogLogSink(opts: WorkerDatadogLogSinkOptions) {
  return new DatadogLogSink({...opts, source: 'worker'});
}

export * from './replicache-mod.js';

export type {
  AuthData,
  MutatorDefs,
  ReadTransaction,
  WriteTransaction,
} from 'reflect-types/src/mod.js';

export {version} from './util/version.js';
