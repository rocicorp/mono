import {getProviderConfig} from './cf.js';
import {cfCall} from 'cloudflare-api/src/fetch.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function getWorkerOptions(yargs: CommonYargsArgv) {
  return yargs
    .positional('name', {
      desc: 'The name of the script',
      type: 'string',
      demandOption: true,
    })
    .positional('component', {
      desc: 'The component of the script to get (e.g. "settings", "environments/prod")',
      type: 'string',
    })
    .option('namespace', {
      desc: 'The namespace of the script.',
      type: 'string',
    })
    .option('resource', {
      desc: 'The worker resource',
      type: 'string',
      options: ['scripts', 'services'],
      default: 'scripts',
    });
}

type GetWorkerHandlerArgs = YargvToInterface<
  ReturnType<typeof getWorkerOptions>
>;

export async function getWorkerHandler(
  yargs: GetWorkerHandlerArgs,
): Promise<void> {
  const config = await getProviderConfig(yargs);
  const {apiKey, accountID} = config;
  const {name, component, namespace, resource} = yargs;

  const base = namespace
    ? `/accounts/${accountID}/workers/dispatch/namespaces/${namespace}/${resource}/${name}`
    : `/accounts/${accountID}/workers/${resource}/${name}`;
  const url = component ? `${base}/${component}` : base;

  console.log(`GET ${url}`);
  const result = await cfCall(apiKey, url);
  console.log(await result.text());
}
