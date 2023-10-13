import {ProviderConfig, getProviderConfig} from './cf.js';
import {FetchResultError, Errors} from 'cloudflare-api/src/fetch.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {DispatchNamespaces} from 'cloudflare-api/src/dispatch-namespaces.js';
import {DNSRecords} from 'cloudflare-api/src/dns-records.js';
import {FallbackOrigin} from 'cloudflare-api/src/fallback-origin.js';
import {WorkerRoutes} from 'cloudflare-api/src/worker-routes.js';
import {publishWorker} from './publish-worker.js';
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
  await publishDispatcherScript(config);

  // The worker must have been created in order to setup the fallback Worker Route.
  await ensureFallbackRoute(config, overwriteFallbacks);

  // This can technically be done in parallel with the rest but we keep it serial for readability.
  await ensureFallbackOrigin(config, fallbackHostname, overwriteFallbacks);
}

const DISPATCHER_NAME = 'dispatcher';

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
  overwriteExisting: boolean,
) {
  const script = DISPATCHER_NAME;
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

async function publishDispatcherScript({
  apiToken,
  accountID,
  dispatchNamespace: namespace,
}: ProviderConfig): Promise<void> {
  await publishWorker({apiToken, accountID}, DISPATCHER_NAME, {
    /* eslint-disable @typescript-eslint/naming-convention */
    bindings: {dispatch_namespaces: [{binding: 'workers', namespace}]},
    // no_minimal_subrequests is required to dispatch to non-namespaced workers by Custom Domain.
    compatibility_flags: ['no_minimal_subrequests'],
    /* eslint-enable @typescript-eslint/naming-convention */
  });
}
