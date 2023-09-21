import {
  CloudflareConfig,
  getCloudflareConfig,
  getZoneDomainName,
} from './cf.js';
import {FetchResultError, cfFetch, ERRORS} from 'cloudflare-api/src/fetch.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {fileURLToPath} from 'url';
import {readFile} from 'node:fs/promises';
import {
  createScriptUploadForm,
  type CfModule,
} from 'cloudflare-api/src/create-script-upload-form.js';
import {DispatchNamespaces} from 'cloudflare-api/src/dispatch-namespaces.js';
import {DNSRecords} from 'cloudflare-api/src/dns-records.js';
import {FallbackOrigin} from 'cloudflare-api/src/fallback-origin.js';
import {WorkerRoutes} from 'cloudflare-api/src/worker-routes.js';
import {sleep} from 'shared/src/sleep.js';

export function publishDispatcherOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('namespace', {
      desc: 'The namespace to which the dispatcher will be bound (where Workers for Platforms are uploaded)',
      type: 'string',
      default: 'mirror',
    })
    .option('fallback-hostname', {
      desc: 'The hostname (before the TLD) to set the fallback origin to.',
      type: 'string',
      default: 'apps',
    })
    .option('script-name', {
      desc: 'The script name of the dispatcher. If none is specified, defaults to `${namespace}-dispatcher`',
      type: 'string',
    })
    .option('overwrite-fallbacks', {
      desc: 'Overwrites an existing fallback route and origin if it is different',
      type: 'boolean',
      default: false,
    });
}

type PublishDispatcherHandlerArgs = YargvToInterface<
  ReturnType<typeof publishDispatcherOptions>
>;

export async function publishDispatcherHandler(
  yargs: PublishDispatcherHandlerArgs,
): Promise<void> {
  const config = await getCloudflareConfig(yargs);
  const zoneName = await getZoneDomainName(config);

  const {namespace, fallbackHostname, overwriteFallbacks} = yargs;
  const scriptName = yargs.scriptName ?? `${namespace}-dispatcher`;

  console.log(`Publishing ${scriptName}`);

  // These must be done serially:
  await ensureDispatchNamespace(config, namespace);

  // The namespace must have been created in order to setup bindings to it in the Worker.
  await publishDispatcherScript(config, namespace, scriptName);

  // The worker must have been created in order to setup the fallback Worker Route.
  await ensureFallbackRoute(config, zoneName, scriptName, overwriteFallbacks);

  // This can technically be done in parallel with the rest but we keep it serial for readability.
  await ensureFallbackOrigin(
    config,
    fallbackHostname,
    zoneName,
    overwriteFallbacks,
  );
}

async function ensureDispatchNamespace(
  {apiKey, accountID}: CloudflareConfig,
  name: string,
): Promise<void> {
  const namespaces = new DispatchNamespaces(apiKey, accountID);
  try {
    const exists = await namespaces.get(name);
    console.log(`"${name}" namespace exists: `, exists);
    return;
  } catch (e) {
    FetchResultError.throwIfCodeIsNot(e, ERRORS.dispatchNamespaceNotFound);
  }
  console.log(`Creating "${name}" namespace`);
  const result = await namespaces.create({name});
  console.log(result);
}

export async function ensureFallbackRoute(
  {apiKey, zoneID}: CloudflareConfig,
  zoneName: string,
  script: string,
  overwriteExisting: boolean,
) {
  const pattern = `*.${zoneName}/*`;
  const resource = new WorkerRoutes(apiKey, zoneID);
  for (const route of await resource.list()) {
    if (route.pattern === pattern) {
      if (route.script === script) {
        console.log(`Route already exists`, route);
        return;
      }
      if (!overwriteExisting) {
        throw new Error(
          `Fallback route is currently set to ${route.script}. Use --overwrite-fallbacks to overwrite it.`,
        );
      }
      console.warn(`Replacing old route to point to ${script}`, route);
      const result = await resource.update(route.id, {pattern, script});
      console.log(result);
      return;
    }
  }
  const result = await resource.create({pattern, script});
  console.log(result);
}

export async function ensureFallbackOrigin(
  {apiKey, zoneID}: CloudflareConfig,
  hostname: string,
  zoneName: string,
  overwrite: boolean,
): Promise<void> {
  const origin = `${hostname}.${zoneName}`;
  const current = new FallbackOrigin(apiKey, zoneID);
  try {
    const existing = await current.get();
    if (existing.origin === origin) {
      console.log(`Fallback Origin is already set to ${origin}`);
      return;
    }
    if (!overwrite) {
      throw new Error(
        `Fallback origin is currently set to ${existing.origin}. Use --overwrite-fallbacks to overwrite it.`,
      );
    }
    console.warn(`Overwriting existing fallback origin ${existing.origin}`);
  } catch (e) {
    FetchResultError.throwIfCodeIsNot(e, ERRORS.resourceNotFound);
  }
  // https://developers.cloudflare.com/cloudflare-for-platforms/cloudflare-for-saas/start/advanced-settings/worker-as-origin/
  console.log(`Creating Fallback Origin DNS record for ${hostname}`);
  try {
    const dnsRecords = new DNSRecords(apiKey, zoneID);
    const dnsResult = await dnsRecords.create({
      type: 'AAAA',
      name: hostname,
      content: '100::',
    });
    console.log(dnsResult);
  } catch (e) {
    FetchResultError.throwIfCodeIsNot(e, ERRORS.recordAlreadyExists); // Assume it is correct.
  }

  console.log(`Setting Fallback Origin to ${origin}`);
  let state = await current.update({origin});
  while (state.status !== 'active') {
    console.log(`Waiting for Fallback origin to become active: `, state);
    await sleep(2000);
    state = await current.get();
  }
  console.log(state);
}

async function publishDispatcherScript(
  {apiKey, accountID}: CloudflareConfig,
  namespace: string,
  name: string,
): Promise<void> {
  const dispatcherScript = await loadDispatcherScript();
  console.log(`Loaded ${name}`, dispatcherScript);

  const main: CfModule = {
    name: 'dispatcher.js',
    content: dispatcherScript,
    type: 'esm',
  };

  /* eslint-disable @typescript-eslint/naming-convention */
  const form = createScriptUploadForm({
    name,
    main,
    bindings: {dispatch_namespaces: [{binding: 'workers', namespace}]},
    compatibility_date: '2023-09-04',
    // no_minimal_subrequests is required to dispatch to non-namespaced workers by Custom Domain.
    compatibility_flags: ['no_minimal_subrequests'],
  });
  /* eslint-enable @typescript-eslint/naming-convention */

  const result = await cfFetch(
    apiKey,
    `/accounts/${accountID}/workers/scripts/${name}`,
    {
      method: 'PUT',
      body: form,
    },
    new URLSearchParams({
      // eslint-disable-next-line @typescript-eslint/naming-convention
      include_subdomain_availability: 'true',
      // pass excludeScript so the whole body of the
      // script doesn't get included in the response
      excludeScript: 'true',
    }),
  );
  console.log(`Publish result:`, result);
}

function loadDispatcherScript(): Promise<string> {
  const dispatcherFile = fileURLToPath(
    new URL('../out/dispatcher.js', import.meta.url),
  );
  console.log('Loading ', dispatcherFile);
  return readFile(dispatcherFile, 'utf-8');
}
