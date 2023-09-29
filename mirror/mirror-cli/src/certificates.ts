import {getProviderConfig} from './cf.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {CertificatePacks} from 'cloudflare-api/src/certificate-packs.js';

export function certificatesOptions(yargs: CommonYargsArgv) {
  return yargs
    .positional('pattern', {
      desc: 'Optional pattern to match when listing or deleting certificates',
      type: 'string',
      conflicts: 'create',
    })
    .option('get', {
      desc: 'Gets a certificate by ID',
      type: 'string',
      conflicts: ['pattern', 'delete'],
    })
    .option('delete', {
      desc: 'Delete matching certificates',
      type: 'boolean',
      conflicts: ['create', 'get'],
    });
}

type CertificatesHandlerArgs = YargvToInterface<
  ReturnType<typeof certificatesOptions>
>;

export async function certificatesHandler(
  yargs: CertificatesHandlerArgs,
): Promise<void> {
  const {pattern = '', delete: deleteCerts = false, get} = yargs;
  const config = await getProviderConfig(yargs);
  const {
    apiToken,
    defaultZone: {zoneID},
  } = config;

  const resource = new CertificatePacks({apiToken, zoneID});

  if (get) {
    const result = await resource.get(get);
    console.log(result);
    return;
  }

  let matched = 0;
  for (const cert of await resource.list(
    new URLSearchParams({['per_page']: '100'}),
  )) {
    const host = cert.hosts.find(host => host.indexOf(pattern) >= 0);
    if (host) {
      console.log(`Matched ${host}`); //, cert);
      matched++;
      if (deleteCerts) {
        const result = await resource.delete(cert.id);
        console.log(`Delete result`, result);
      }
    }
  }
  console.log(
    `Matched${deleteCerts ? ' and deleted ' : ' '}${matched} certificates`,
  );

  const quota = await resource.getQuota();
  console.log(`Current quota:`, quota);
}
