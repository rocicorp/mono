import {DatadogLogSink} from 'datadog';

export function createWorkerDatadogLogSink(opts: WorkerDatadogLogSinkOptions) {
  return new DatadogLogSink({...opts, source: 'worker'});
}

export type WorkerDatadogLogSinkOptions = {
  apiKey: string;
  service?: string | undefined;
  host?: string | undefined;
  version?: string | undefined;
};
