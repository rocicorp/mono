/* eslint-disable @typescript-eslint/naming-convention */
import {readFile} from 'node:fs/promises';
import * as path from 'node:path';
import {fileURLToPath} from 'node:url';
import {cfFetch} from './cf-fetch.js';
import {CfModule, createWorkerUploadForm} from './create-worker-upload-form.js';
import {Migration, getMigrationsToUpload} from './get-migrations-to-upload.js';

type Config = {
  accountID: string;
  scriptName: string;
  apiToken: string;
};

export async function createWorker(
  {accountID, scriptName, apiToken}: Config,
  mainModule: CfModule,
  modules: CfModule[],
) {
  const cfMigrations = await getMigrationsToUpload(scriptName, apiToken, {
    accountId: accountID,
    config: {migrations},
    legacyEnv: false,
    env: undefined,
  });

  const form = createWorkerUploadForm({
    name: scriptName,
    main: mainModule, // await createCfModule('worker.js'),
    bindings: {
      durable_objects: {
        bindings: [
          {name: 'roomDO', class_name: 'RoomDO'},
          {name: 'authDO', class_name: 'AuthDO'},
        ],
      },
    },
    modules,
    migrations: cfMigrations,
    compatibility_date: '2023-05-18',
  });

  const resource = `/accounts/${accountID}/workers/scripts/${scriptName}`;
  const searchParams = new URLSearchParams({
    include_subdomain_availability: 'true',
    // pass excludeScript so the whole body of the
    // script doesn't get included in the response
    excludeScript: 'true',
  });
  await cfFetch(
    apiToken,
    resource,
    {
      method: 'PUT',
      body: form,
    },
    searchParams,
  );
}

export async function enableSubdomain(conf: Config) {
  const {accountID, scriptName, apiToken} = conf;
  const resource = `/accounts/${accountID}/workers/scripts/${scriptName}/subdomain`;

  await cfFetch(apiToken, resource, {
    method: 'POST',
    headers: {
      'Content-type': 'application/json',
    },
    body: JSON.stringify({enabled: true}),
  });
}

const migrations: Migration[] = [
  {
    tag: 'v1',
    new_classes: ['RoomDO', 'AuthDO'],
  },
];

function workerModuleContent(appFileName: string) {
  return `
import {createReflectServer} from './reflect-server.js';
import {default as makeOptions} from './${appFileName}';
const {worker, RoomDO, AuthDO} = createReflectServer(makeOptions);
export {worker as default, RoomDO, AuthDO};
`;
}

function getServerContent() {
  // TODO(arv): This is a hack to get to the server source code.
  const dirname = path.dirname(fileURLToPath(import.meta.url));
  const serverPath = path.join(dirname, '../out/data/reflect-server.js');
  return readFile(serverPath, 'utf8');
}

export async function publish(
  config: Config,
  sourceModule: CfModule,
  sourcemapModule: CfModule,
  appName: string,
) {
  console.log('publishing', appName);
  const workerModule = {
    name: 'worker.js',
    content: workerModuleContent(sourceModule.name),
    type: 'esm',
  } as const;
  const serverContent = await getServerContent();
  const serverModule = {
    name: 'reflect-server.js',
    content: serverContent,
    type: 'esm',
  } as const;
  const modules: CfModule[] = [sourceModule, sourcemapModule, serverModule];
  await createWorker(config, workerModule, modules);

  // TODO(arv): Set up the custom domain. The below code does not seem to do
  // enough.
  // reflect.net/wrangler.toml has:
  // route = { pattern = "reflect-server.net", custom_domain = true }
  await enableSubdomain(config);
}
