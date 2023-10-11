import {ProviderConfig, getProviderConfig} from './cf.js';
import {FetchResultError, cfFetch, Errors} from 'cloudflare-api/src/fetch.js';
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
      desc: 'The dispatch namespace (e.g. "prod" or "sand"',
      type: 'string',
      demandOption: true,
    })
    .option('fallback-hostname', {
      desc: 'The hostname (before the TLD) to set the fallback origin to.',
      type: 'string',
      default: 'apps',
    })
    .option('script-name', {
      desc: 'The script name of the dispatcher. If none is specified, defaults to `dispatcher`',
      type: 'string',
      default: 'dispatcher',
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
  const config = await getProviderConfig(yargs);

  const {namespace, fallbackHostname, overwriteFallbacks} = yargs;
  const scriptName = yargs.scriptName ?? `${namespace}-dispatcher`;

  console.log(`Publishing ${scriptName}`);

  // These must be done serially:
  await ensureDispatchNamespace(config);

  // The namespace must have been created in order to setup bindings to it in the Worker.
  await publishDispatcherScript(config, scriptName);

  // The worker must have been created in order to setup the fallback Worker Route.
  await ensureFallbackRoute(config, scriptName, overwriteFallbacks);

  // This can technically be done in parallel with the rest but we keep it serial for readability.
  await ensureFallbackOrigin(config, fallbackHostname, overwriteFallbacks);
}

async function ensureDispatchNamespace({
  apiToken,
  accountID,
  dispatchNamespace: name,
}: ProviderConfig): Promise<void> {
  const namespaces = new DispatchNamespaces({apiToken, accountID});
  try {
    const exists = await namespaces.get(name);
    console.log(`"${name}" namespace exists: `, exists);
    return;
  } catch (e) {
    FetchResultError.throwIfCodeIsNot(e, Errors.DispatchNamespaceNotFound);
  }
  console.log(`Creating "${name}" namespace`);
  const result = await namespaces.create({name});
  console.log(result);
}

export async function ensureFallbackRoute(
  {apiToken, defaultZone: {zoneID, zoneName}}: ProviderConfig,
  script: string,
  overwriteExisting: boolean,
) {
  const pattern = `*.${zoneName}/*`;
  const resource = new WorkerRoutes({apiToken, zoneID});
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
  {apiToken, defaultZone: {zoneID, zoneName}}: ProviderConfig,
  hostname: string,
  overwrite: boolean,
): Promise<void> {
  const origin = `${hostname}.${zoneName}`;
  const current = new FallbackOrigin({apiToken, zoneID});
  try {
    const existing = await current.get();
    if (overwrite) {
      console.warn(`Overwriting existing fallback origin ${existing.origin}`);
    } else if (existing.origin === origin) {
      console.log(`Fallback Origin is already set to ${origin}`);
      return;
    } else {
      throw new Error(
        `Fallback origin is currently set to ${existing.origin}. Use --overwrite-fallbacks to overwrite it.`,
      );
    }
  } catch (e) {
    FetchResultError.throwIfCodeIsNot(e, Errors.ResourceNotFound);
  }
  // https://developers.cloudflare.com/cloudflare-for-platforms/cloudflare-for-saas/start/advanced-settings/worker-as-origin/
  console.log(`Creating Fallback Origin DNS record for ${origin}`);
  const originRecord = {
    type: 'AAAA',
    name: origin,
    content: '100::',
    proxied: true,
    comment: 'Managed by Rocicorp (reflect.net)',
    tags: ['managed:rocicorp'],
  };
  const dnsRecords = new DNSRecords({apiToken, zoneID});
  try {
    const dnsResult = await dnsRecords.create(originRecord);
    console.log(dnsResult);
  } catch (e) {
    FetchResultError.throwIfCodeIsNot(e, Errors.RecordAlreadyExists);

    const existing = await dnsRecords.list(
      new URLSearchParams({name: origin, type: 'AAAA'}),
    );
    if (existing.length !== 1) {
      throw new Error(
        `Unexpected number of existing records for ${origin}: ${JSON.stringify(
          existing,
        )}`,
      );
    }
    console.log(
      `Overwriting existing DNSRecord:`,
      existing[0],
      'with',
      originRecord,
    );
    await dnsRecords.update(existing[0].id, originRecord);
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
  {apiToken, accountID, dispatchNamespace: namespace}: ProviderConfig,
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
    apiToken,
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
