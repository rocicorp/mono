/* eslint-disable @typescript-eslint/naming-convention */
import {fetch} from 'undici';
import {createWorkerUploadForm} from './create-worker-upload-form';
import type {CfWorkerInit} from './worker';
import {readFileSync} from 'fs';
import path from 'path';
import type {
  CommonYargsArgv,
  StrictYargsOptionsToInterface,
} from './yarg-types';

export function publishOptions(yargs: CommonYargsArgv) {
  return yargs.option('name', {
    describe: 'Name of the worker',
    type: 'string',
    requiresArg: true,
  });
}

export async function publishHandler(
  yargs: StrictYargsOptionsToInterface<typeof publishOptions>,
) {
  const modules: [] = [];
  const resolvedEntryPointPath = '../dryout/index.js';
  const content = readFileSync(resolvedEntryPointPath, {
    encoding: 'utf-8',
  });

  const bindings: CfWorkerInit['bindings'] = {
    vars: undefined,
    kv_namespaces: undefined,
    wasm_modules: undefined,
    text_blobs: undefined,
    data_blobs: undefined,
    queues: undefined,
    r2_buckets: undefined,
    d1_databases: undefined,
    services: undefined,
    analytics_engine_datasets: undefined,
    dispatch_namespaces: undefined,
    mtls_certificates: undefined,
    unsafe: undefined,
    logfwdr: undefined,
    durable_objects: {
      bindings: [
        {
          name: 'roomDO',
          class_name: 'RoomDO',
        },
        {
          name: 'authDO',
          class_name: 'AuthDO',
        },
      ],
    },
  };

  const worker: CfWorkerInit = {
    name: yargs.name,
    main: {
      name: path.basename(resolvedEntryPointPath),
      content,
      type: 'commonjs',
    },
    bindings,
    migrations: undefined,
    modules,
    compatibility_date: '2022-06-03',
    compatibility_flags: [],
    usage_model: undefined,
    keepVars: undefined,
    logpush: undefined,
  };

  await fetch(`http://0.0.0.0:8787`, {
    method: 'PUT',
    body: createWorkerUploadForm(worker),
    headers: {},
  });
}
