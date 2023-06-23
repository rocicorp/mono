/* eslint-disable @typescript-eslint/naming-convention */
import type {Firestore} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {HttpsError} from 'firebase-functions/v2/https';
import * as schema from 'mirror-schema/src/reflect-server.js';
import assert from 'node:assert';
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

function assertAllModulesHaveUniqueNames(modules: Iterable<CfModule>) {
  const names = new Set<string>();
  for (const m of modules) {
    assert(!names.has(m.name), `Duplicate module name: ${m.name}`);
    names.add(m.name);
  }
}

export async function publish(
  firestore: Firestore,
  storage: Storage,
  bucketName: string,
  config: Config,
  appModule: CfModule,
  appSourcemapModule: CfModule,
  appName: string,
  desiredVersion: string,
) {
  console.log('publishing', appName);

  const [serverModule, ...otherServerModules] = await getServerModules(
    firestore,
    storage,
    bucketName,
    desiredVersion,
  );

  let workerModule: CfModule | undefined;
  const otherModules: CfModule[] = [];
  for (const m of otherServerModules) {
    if (m.name === 'worker.template.js') {
      const content = m.content
        .replaceAll('<REFLECT_SERVER>', serverModule.name)
        .replaceAll('<APP>', appModule.name);
      workerModule = {content, name: 'worker.js', type: 'esm'};
    } else {
      otherModules.push(m);
    }
  }
  assert(workerModule);

  const modules: CfModule[] = [
    appModule,
    appSourcemapModule,
    serverModule,
    ...otherModules,
  ];

  // Make sure that all the names are unique.
  assertAllModulesHaveUniqueNames([workerModule, ...modules]);

  console.log('publishing', appName);
  await createWorker(config, workerModule, modules);

  // TODO(arv): Set up the custom domain. The below code does not seem to do
  // enough.
  // reflect.net/wrangler.toml has:
  // route = { pattern = "reflect-server.net", custom_domain = true }
  await enableSubdomain(config);
}

async function getServerModules(
  firestore: Firestore,
  storage: Storage,
  bucketName: string,
  desiredVersion: string,
): Promise<CfModule[]> {
  // TODO(arv): Find compatible version.
  const version = desiredVersion;

  const docRef = firestore
    .doc(schema.reflectServerPath(version))
    .withConverter(schema.reflectServerDataConverter);

  const serverModule = await firestore.runTransaction(
    async txn => {
      const doc = await txn.get(docRef);
      const {exists} = doc;
      if (!exists) {
        throw new HttpsError('not-found', `Version ${version} does not exist`);
      }

      return doc.data();
    },
    {readOnly: true},
  );
  assert(serverModule);

  const modules = [serverModule.main, ...serverModule.modules];
  const bucket = storage.bucket(bucketName);

  return Promise.all(
    modules.map(async module => {
      const {name, filename, type} = module;
      const content = await bucket.file(filename).download();
      return {name, content: content[0].toString('utf-8'), type};
    }),
  );
}
