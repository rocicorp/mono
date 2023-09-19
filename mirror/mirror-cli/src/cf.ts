import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {SecretManagerServiceClient} from '@google-cloud/secret-manager';
import {cfFetch} from 'cloudflare-api/src/fetch.js';

type Account = {
  accountID: string;
  zoneID: string;
};

const CF_ACCOUNTS: Record<string, Account> = {
  prod: {
    accountID: '085f6d8eb08e5b23debfb08b21bda1eb', // Rocicorp LLC
    zoneID: '1b044253688b6ddb8e67738539a2b6d0', // reflect-server.net
  },
  sandbox: {
    accountID: 'b50bbcc3bb7e6f0383c8048e1978c660', // Rocicorp DEV
    zoneID: '01ebb92bad69151e567a5bfc2e871f55', // reflect-server.dev
  },
} as const;

export type CloudflareConfig = Account & {
  apiKey: string;
};

export async function getCloudflareConfig(
  yargs: YargvToInterface<CommonYargsArgv>,
): Promise<CloudflareConfig> {
  const {stack} = yargs;
  const account = CF_ACCOUNTS[stack];
  if (!account) {
    throw new Error(`No CF Account configured for stack "${stack}"`);
  }
  const secrets = new SecretManagerServiceClient();
  const name = `projects/reflect-mirror-${stack}/secrets/CLOUDFLARE_API_TOKEN/versions/latest`;
  const [{payload}] = await secrets.accessSecretVersion({name});
  if (!payload || !payload.data) {
    throw new Error(`No data for API key secret ${name}`);
  }
  const {data} = payload;
  const apiKey =
    typeof data === 'string' ? data : Buffer.from(data).toString('utf-8');
  return {...account, apiKey};
}

type Zone = {
  id: string;
  name: string;
};

export async function getZoneDomainName({
  apiKey,
  zoneID,
}: CloudflareConfig): Promise<string> {
  const zone = await cfFetch<Zone>(apiKey, `/zones/${zoneID}`, {
    method: 'GET',
    headers: {'Content-Type': 'application/json'},
  });
  return zone.name;
}
