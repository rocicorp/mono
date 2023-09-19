import {getCloudflareConfig} from './cf.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {DNSRecords} from 'cloudflare-api/src/dns-records.js';

export function dnsRecordsOptions(yargs: CommonYargsArgv) {
  return yargs
    .positional('search', {
      desc: 'Optional search to match when listing or deleting records',
      type: 'string',
    })
    .option('delete', {
      desc: 'Delete matching records',
      type: 'boolean',
      default: false,
    });
}

type DnsRecordsHandlerArgs = YargvToInterface<
  ReturnType<typeof dnsRecordsOptions>
>;

export async function dnsRecordsHandler(
  yargs: DnsRecordsHandlerArgs,
): Promise<void> {
  const {search, delete: deleteRecords} = yargs;
  const {apiKey, zoneID} = await getCloudflareConfig(yargs);
  const resource = new DNSRecords(apiKey, zoneID);
  const query = search ? new URLSearchParams({search}) : undefined;

  for (const record of await resource.list(query)) {
    console.log(`Matched ${record.name}`, record);
    if (deleteRecords) {
      const {id} = record;
      const result = await resource.delete(id);
      console.log(`Delete result`, result);
    }
  }
}
