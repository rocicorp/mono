import {CloudflareConfig, getCloudflareConfig} from './cf.js';
import {cfFetch} from 'cloudflare-api/src/fetch.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function publishCustomDomainsOptions(yargs: CommonYargsArgv) {
  return yargs
    .positional('script-name', {
      desc: 'Script name to publish custom domains for',
      type: 'string',
      demandOption: true,
    })
    .positional('domains', {
      desc: 'The domains to publish to, e.g. "app-name.team-name.reflect-server.net"',
      array: true,
      type: 'string',
      demandOption: true,
    });
}

type PublishCustomDomainsHandlerArgs = YargvToInterface<
  ReturnType<typeof publishCustomDomainsOptions>
>;

export async function publishCustomDomainsHandler(
  yargs: PublishCustomDomainsHandlerArgs,
): Promise<void> {
  const config = await getCloudflareConfig(yargs);
  const {scriptName, domains} = yargs;
  await publishCustomDomains(config, scriptName, ...domains);
}

// Original file was here:
// https://github.com/cloudflare/workers-sdk/blob/a728876e607635081cd1ed00d06b7af86e7efd49/packages/wrangler/src/deploy/deploy.ts#L170
//
// The original file allowed an interactive mode if the terminal was a TTY. We
// remove that to simplify the code and it is always running in non-interactive
// mode. Therefore existing origins / dns records are not indicative of errors,
// so we aggressively update rather than aggressively fail

export async function publishCustomDomains(
  {apiKey, accountID}: CloudflareConfig,
  scriptName: string,
  ...hostnames: string[]
): Promise<void> {
  /* eslint-disable @typescript-eslint/naming-convention */
  const body = {
    override_scope: true,
    override_existing_origin: true,
    override_existing_dns_record: true,
    origins: hostnames.map(hostname => ({hostname})),
  };
  /* eslint-enable @typescript-eslint/naming-convention */

  console.log(`Publishing ${scriptName} to `, body);

  // deploy to domains
  const results = await cfFetch(
    apiKey,
    `/accounts/${accountID}/workers/scripts/${scriptName}/domains/records`,
    {
      method: 'PUT',
      body: JSON.stringify(body),
      headers: {
        'Content-Type': 'application/json',
      },
    },
  );

  console.log(results);
}
