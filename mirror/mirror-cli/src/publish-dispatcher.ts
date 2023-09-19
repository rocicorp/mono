import {
  CloudflareConfig,
  getCloudflareConfig,
  getZoneDomainName,
} from './cf.js';
import {FetchResultError, cfFetch} from 'cloudflare-api/src/fetch.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {fileURLToPath} from 'url';
import {readFile} from 'node:fs/promises';
import {
  createScriptUploadForm,
  type CfModule,
} from 'cloudflare-api/src/create-script-upload-form.js';
import {publishCustomDomains} from './publish-custom-domains.js';
import {sleep} from 'shared/src/sleep.js';

export function publishDispatcherOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('namespace', {
      desc: 'The namespace to which the dispatcher will be bound (where Workers for Platforms are uploaded)',
      type: 'string',
      default: 'mirror',
    })
    .option('hostname', {
      desc:
        'The hostname (before the TLD) on which the dispatcher will run. This will be configured as the fallback origin for WfP Custom Hostnames. ' +
        'If none is specified, defaults to `${namespace}-dispatcher`',
      type: 'string',
    })
    .option('script-name', {
      desc: 'The script name of the dispatcher. If none is specified, defaults to `${namespace}-dispatcher`',
      type: 'string',
    })
    .option('overwrite-fallback-origin', {
      desc: 'Overwrites an existing fallback origin if it is different from ${hostname}.${tld}',
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
  const tld = await getZoneDomainName(config);

  const {namespace, overwriteFallbackOrigin} = yargs;
  const scriptName = yargs.scriptName ?? `${namespace}-dispatcher`;
  const hostname = yargs.hostname ?? `${namespace}-dispatcher`;
  const domainName = `${hostname}.${tld}`;

  console.log(`Publishing ${scriptName} to ${domainName}`);

  // These must be done serially:
  await ensureDispatchNamespace(config, namespace);
  // The namespace must have been created in order to setup bindings to it in the Worker.
  await publishDispatcherScript(config, namespace, scriptName, domainName);
  // The Worker must have been published in order to point the custom domain to it.
  await publishCustomDomains(config, scriptName, domainName);
  // The custom domain must have been created in order to use it as the Fallback origin.
  await ensureFallbackOrigin(config, domainName, overwriteFallbackOrigin);
}

async function ensureDispatchNamespace(
  {apiKey, accountID}: CloudflareConfig,
  name: string,
): Promise<void> {
  try {
    const exists = await cfFetch(
      apiKey,
      `/accounts/${accountID}/workers/dispatch/namespaces/${name}`,
    );
    console.log(`"${name}" namespace exists: `, exists);
    return;
  } catch (e) {
    if (e instanceof FetchResultError && e.codes().includes(100119)) {
      // workers.api.error.dispatch_namespace_not_found
      // Continue on to create the namespace.
    } else {
      throw e;
    }
  }
  console.log(`Creating "${name}" namespace`);
  const result = await cfFetch(
    apiKey,
    `/accounts/${accountID}/workers/dispatch/namespaces`,
    {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name}),
    },
  );
  console.log(result);
}

type FallbackOrigin = {
  origin: string;
  status: string;
  errors: string[];
};

export async function ensureFallbackOrigin(
  {apiKey, zoneID}: CloudflareConfig,
  origin: string,
  overwrite: boolean,
): Promise<void> {
  const resource = `/zones/${zoneID}/custom_hostnames/fallback_origin`;
  try {
    const existing = await cfFetch<FallbackOrigin>(apiKey, resource);
    if (existing.origin !== origin) {
      if (!overwrite) {
        throw new Error(
          `Fallback origin is currently set to ${existing.origin}. Use --overwrite-fallback-origin to overwrite it.`,
        );
      }
      console.warn(`Overwriting existing fallback origin ${existing.origin}`);
    }
  } catch (e) {
    if (e instanceof FetchResultError && e.codes().includes(1551)) {
      // Resource not found.
      // Continue on to set the fallback origin.
    } else {
      throw e;
    }
  }
  console.log(`Setting fallback origin: ${origin}`);
  let fallbackOrigin = await cfFetch<FallbackOrigin>(apiKey, resource, {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({origin}),
  });
  while (fallbackOrigin.status !== 'active') {
    console.log(
      `Waiting for Fallback origin to become active: `,
      fallbackOrigin,
    );
    await sleep(2000);
    fallbackOrigin = await cfFetch<FallbackOrigin>(apiKey, resource);
  }
  console.log(fallbackOrigin);
}

async function publishDispatcherScript(
  {apiKey, accountID}: CloudflareConfig,
  namespace: string,
  name: string,
  _2: string,
): Promise<void> {
  const dispatcherScript = await loadDispatcherScript();
  console.log(`Loaded ${name}`, dispatcherScript);

  const main: CfModule = {
    name: 'dispatcher.js',
    content: dispatcherScript,
    type: 'esm',
  };

  const form = createScriptUploadForm({
    name,
    main,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    bindings: {dispatch_namespaces: [{binding: 'workers', namespace}]},
    // eslint-disable-next-line @typescript-eslint/naming-convention
    compatibility_date: '2023-09-04',
  });

  const result = await cfFetch(
    apiKey,
    `/accounts/${accountID}/workers/dispatch/namespaces/${namespace}/scripts/${name}`,
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
