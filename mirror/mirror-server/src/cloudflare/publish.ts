import type {Storage} from 'firebase-admin/storage';
import {nanoid} from 'nanoid';
import {cfFetch} from './cf-fetch.js';
import type {Config} from './config.js';
import {CfModule, createWorkerUploadForm} from './create-worker-upload-form.js';
import {Migration, getMigrationsToUpload} from './get-migrations-to-upload.js';
import {publishCustomDomains} from './publish-custom-domains.js';
import {submitSecret} from './submit-secret.js';
import {submitTriggers} from './submit-triggers.js';
import {logger} from 'firebase-functions';
import type {ModuleRef} from 'mirror-schema/src/module.js';
import {ModuleAssembler} from './module-assembler.js';

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
      // eslint-disable-next-line @typescript-eslint/naming-convention
      durable_objects: {
        bindings: [
          // eslint-disable-next-line @typescript-eslint/naming-convention
          {name: 'roomDO', class_name: 'RoomDO'},
          // eslint-disable-next-line @typescript-eslint/naming-convention
          {name: 'authDO', class_name: 'AuthDO'},
        ],
      },
    },
    modules,
    migrations: cfMigrations,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    compatibility_date: '2023-05-18',
  });

  const resource = `/accounts/${accountID}/workers/scripts/${scriptName}`;
  const searchParams = new URLSearchParams({
    // eslint-disable-next-line @typescript-eslint/naming-convention
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

const migrations: Migration[] = [
  {
    tag: 'v1',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    new_classes: ['RoomDO', 'AuthDO'],
  },
];

export async function publish(
  storage: Storage,
  config: Config,
  appName: string,
  appModules: ModuleRef[],
  serverModules: ModuleRef[],
): Promise<string> {
  const assembler = new ModuleAssembler(appModules, serverModules);
  const modules = await assembler.assemble(storage);

  logger.log(`publishing ${appName}.reflect-server.net (${config.scriptName})`);
  await createWorker(config, modules[0], modules.slice(1));

  let reflectAuthApiKey = process.env.REFLECT_AUTH_API_KEY;
  if (!reflectAuthApiKey) {
    // TODO(arv): Figure this out once and for all.
    logger.log('Missing REFLECT_AUTH_API_KEY, using a random one');
    reflectAuthApiKey = nanoid();
  }

  const hostname = `${appName}.reflect-server.net`;
  await Promise.all([
    publishCustomDomains(config, hostname),
    submitSecret(config, 'REFLECT_AUTH_API_KEY', reflectAuthApiKey),
    submitTriggers(config, '*/5 * * * *'),
  ]);

  return hostname;
}
