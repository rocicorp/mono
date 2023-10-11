import {getProviderConfig} from './cf.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {DNSRecords, type DNSRecord} from 'cloudflare-api/src/dns-records.js';

export function migrateDnsCommentsToTagsOptions(yargs: CommonYargsArgv) {
  return yargs.option('dry-run', {
    desc: 'Output actions to console instead of committing them',
    type: 'boolean',
    default: true,
  });
}

type MigrateDnsCommentsToTagsHandlerArgs = YargvToInterface<
  ReturnType<typeof migrateDnsCommentsToTagsOptions>
>;

export async function migrateDnsCommentsToTagsHandler(
  yargs: MigrateDnsCommentsToTagsHandlerArgs,
): Promise<void> {
  const {dryRun} = yargs;
  const {
    apiToken,
    defaultZone: {zoneID},
  } = await getProviderConfig(yargs);
  const resource = new DNSRecords({apiToken, zoneID});
  const query = new URLSearchParams({['comment.startswith']: '|'});

  for (const record of await resource.list(query)) {
    const {id, name, comment} = record;
    console.log(`Matched ${name}`, record);

    const tags = comment.split('|').filter(val => val.length > 0);
    tags.push('managed:rocicorp');

    const patch: Partial<DNSRecord> = {
      comment: 'Managed by Rocicorp (reflect.net)',
      tags,
    };

    if (dryRun) {
      console.info(`Would PATCH ${name}`, patch);
    } else {
      await resource.patch(id, patch);
    }
  }
}
