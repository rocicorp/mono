import type {Storage} from 'firebase-admin/storage';
import {nanoid} from 'nanoid';
import type {Config} from './config.js';
import {
  CfModule,
  CfVars,
  createScriptUploadForm,
} from 'cloudflare-api/src/create-script-upload-form.js';
import {Script, GlobalScript} from 'cloudflare-api/src/scripts.js';
import {Migration, getMigrationsToUpload} from './get-migrations-to-upload.js';
import {publishCustomDomains} from './publish-custom-domains.js';
import {submitSecret} from './submit-secret.js';
import {submitTriggers} from './submit-triggers.js';
import {logger} from 'firebase-functions';
import type {ModuleRef} from 'mirror-schema/src/module.js';
import {ModuleAssembler} from './module-assembler.js';
import type {
  DeploymentOptions,
  DeploymentSecrets,
} from 'mirror-schema/src/deployment.js';

export async function createScript(
  script: Script,
  mainModule: CfModule,
  modules: CfModule[],
  vars: CfVars,
) {
  const cfMigrations = await getMigrationsToUpload(script, {
    config: {migrations},
  });

  const form = createScriptUploadForm({
    name: script.name,
    main: mainModule, // await createCfModule('worker.js'),
    bindings: {
      vars,
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

  const searchParams = new URLSearchParams({
    // eslint-disable-next-line @typescript-eslint/naming-convention
    include_subdomain_availability: 'true',
    // pass excludeScript so the whole body of the
    // script doesn't get included in the response
    excludeScript: 'true',
  });
  await script.upload(form, searchParams);
}

const migrations: Migration[] = [
  {
    tag: 'v1',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    new_classes: ['RoomDO', 'AuthDO'],
  },
];

// eslint-disable-next-line require-yield
export async function* publish(
  storage: Storage,
  config: Config,
  appName: string,
  teamLabel: string,
  hostname: string,
  options: DeploymentOptions,
  secrets: DeploymentSecrets,
  appModules: ModuleRef[],
  serverModules: ModuleRef[],
): AsyncGenerator<string> {
  const assembler = new ModuleAssembler(
    appName,
    teamLabel,
    config.scriptName,
    appModules,
    serverModules,
  );
  const modules = await assembler.assemble(storage);

  logger.log(`publishing ${hostname} (${config.scriptName})`);

  const script = new GlobalScript(
    config.apiToken,
    config.accountID,
    config.scriptName,
  );

  await createScript(script, modules[0], modules.slice(1), options.vars);

  let reflectAuthApiKey = process.env.REFLECT_AUTH_API_KEY;
  if (!reflectAuthApiKey) {
    // TODO(arv): Figure this out once and for all.
    logger.log('Missing REFLECT_AUTH_API_KEY, using a random one');
    reflectAuthApiKey = nanoid();
  }

  await Promise.all([
    publishCustomDomains(script, hostname),
    submitTriggers(script, '*/5 * * * *'),
    ...Object.entries(secrets).map(([name, value]) =>
      submitSecret(script, name, value),
    ),
  ]);
}
