import {DatadogLogSink} from 'datadog';

export type {AuthHandler} from './server/auth.js';
export type {DisconnectHandler} from './server/disconnect.js';
export {
  datadogLogging,
  datadogMetrics,
  defaultConsoleLogSink,
  logFilter,
  logLevel,
  newOptionsBuilder,
  type BuildableOptionsEnv,
} from './server/options.js';
export {
  ReflectServerBaseEnv,
  ReflectServerOptions,
  createReflectServer,
} from './server/reflect.js';

export type WorkerDatadogLogSinkOptions = {
  apiKey: string;
  service?: string | undefined;
  host?: string | undefined;
};

export function createWorkerDatadogLogSink(opts: WorkerDatadogLogSinkOptions) {
  return new DatadogLogSink({...opts, source: 'worker'});
}
