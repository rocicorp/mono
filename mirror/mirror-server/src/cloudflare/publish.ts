import type {Storage} from 'firebase-admin/storage';
import {nanoid} from 'nanoid';
import {cfFetch} from './cf-fetch.js';
import type {Config, ZoneConfig} from './config.js';
import {
  CfModule,
  CfVars,
  createScriptUploadForm,
} from 'cloudflare-api/src/create-script-upload-form.js';
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
import {getCertificatePack} from './get-certificate-pack.js';
import {HttpsError} from 'firebase-functions/v2/https';
import {sleep} from 'shared/src/sleep.js';

export async function createScript(
  {accountID, scriptName, apiToken}: Config,
  mainModule: CfModule,
  modules: CfModule[],
  vars: CfVars,
) {
  const cfMigrations = await getMigrationsToUpload(scriptName, apiToken, {
    accountId: accountID,
    config: {migrations},
    legacyEnv: false,
    env: undefined,
  });

  const form = createScriptUploadForm({
    name: scriptName,
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

const POLL_CERTIFICATE_STATUS_INTERVAL = 5000;

export async function* publish(
  storage: Storage,
  config: Config,
  appName: string,
  teamSubdomain: string,
  hostname: string,
  options: DeploymentOptions,
  secrets: DeploymentSecrets,
  appModules: ModuleRef[],
  serverModules: ModuleRef[],
): AsyncGenerator<string> {
  const assembler = new ModuleAssembler(
    appName,
    teamSubdomain,
    config.scriptName,
    appModules,
    serverModules,
  );
  const modules = await assembler.assemble(storage);

  logger.log(`publishing ${hostname} (${config.scriptName})`);
  await createScript(config, modules[0], modules.slice(1), options.vars);

  let reflectAuthApiKey = process.env.REFLECT_AUTH_API_KEY;
  if (!reflectAuthApiKey) {
    // TODO(arv): Figure this out once and for all.
    logger.log('Missing REFLECT_AUTH_API_KEY, using a random one');
    reflectAuthApiKey = nanoid();
  }

  const [customDomains] = await Promise.all([
    publishCustomDomains(config, hostname),
    submitTriggers(config, '*/5 * * * *'),
    ...Object.entries(secrets).map(([name, value]) =>
      submitSecret(config, name, value),
    ),
  ]);

  const customDomainsWithHostname = customDomains.filter(
    domain => domain.hostname === hostname,
  );
  if (customDomainsWithHostname.length !== 1) {
    throw new HttpsError('internal', `No CustomDomain for ${hostname}`);
  }
  const customDomain = customDomainsWithHostname[0];
  if (!customDomain.cert_id) {
    logger.warn(
      `Returned CustomDomain for ${hostname} does not have a cert_id`,
      customDomain,
    );
    return;
  }

  const zoneConfig: ZoneConfig = {
    apiToken: config.apiToken,
    zoneID: customDomain.zone_id,
  };
  // Poll the status of the hostname certificate until it is 'active'.
  for (let lastStatus = undefined; ; ) {
    const cert = await getCertificatePack(zoneConfig, customDomain.cert_id);
    if (cert.status === 'active') {
      // Common case: certificate already exists.
      logger.info(`Certificate for ${hostname} is active.`);
      break;
    }
    if (cert.status === 'initializing' || cert.status.startsWith('pending_')) {
      if (!lastStatus) {
        // This gets written as a DEPLOYING message and is surfaced to users.
        yield `Waiting for TLS to propagate.\n` +
          `    This can take a few minutes the first time you publish a new app.\n` +
          `    ☕ Go get yourself a coffee and it'll be done when you get back.`;
      }
      if (cert.status !== lastStatus) {
        logger.info(`Certificate for ${hostname} is ${cert.status}`, cert);
        lastStatus = cert.status;
      }
    } else {
      throw new HttpsError(
        'internal',
        `Unexpected certificate status: ${cert.status}`,
      );
    }
    await sleep(POLL_CERTIFICATE_STATUS_INTERVAL);
  }
}
