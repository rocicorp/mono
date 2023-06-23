export type {ReflectOptions} from './client/options.js';
export {Reflect} from './client/reflect.js';
// TODO(arv): Only export the types that are actually used.
// https://github.com/rocicorp/mono/issues/362
export * from 'replicache';

import {DatadogLogSink, DatadogLogSinkOptions} from 'datadog';

export type ClientDatadogLogSinkOptions = {
  clientToken: string;
  service?: string | undefined;
};

export function createClientDatadogLogSink(opts: ClientDatadogLogSinkOptions) {
  const opts2: DatadogLogSinkOptions = {
    apiKey: opts.clientToken,
    service: opts.service,
    host: location.host,
    // This has to be set to 'browser' so the server thinks we are the Datadog
    // browser SDK and we get the extra special UA/IP/GEO parsing goodness.
    source: 'browser',
  };
  return new DatadogLogSink(opts2);
}

export type {
  AuthData,
  MutatorDefs,
  ReadTransaction,
  WriteTransaction,
} from 'reflect-types';

// Export the deprecated version to shadow the version from replicache
export {version} from './client/version.js';
