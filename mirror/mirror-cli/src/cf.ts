import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {SecretManagerServiceClient} from '@google-cloud/secret-manager';
import {assert} from 'shared/src/asserts.js';

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

export async function cfCall<ResponseType = unknown>(
  config: {apiKey: string},
  resource: string,
  init: RequestInit = {},
  searchParams?: URLSearchParams,
): Promise<ResponseType> {
  assert(resource.startsWith('/'), 'resource must start with /');
  const base = 'https://api.cloudflare.com/client/v4';
  const queryString = searchParams ? `?${searchParams.toString()}` : '';

  const url = `${base}${resource}${queryString}`;

  const response = await fetch(url, {
    ...init,
    headers: {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      Authorization: `Bearer ${config.apiKey}`,
      ...init?.headers,
    },
  });
  return (await response.json()) as ResponseType;
}

export async function cfFetch<ResponseType = unknown>(
  config: {apiKey: string},
  resource: string,
  init: RequestInit = {},
  searchParams?: URLSearchParams,
): Promise<ResponseType> {
  const json = await cfCall<FetchResult<ResponseType>>(
    config,
    resource,
    init,
    searchParams,
  );
  if (json.success) {
    return json.result;
  }
  throw new Error(`Error returned for ${resource}: ${JSON.stringify(json)}`);
}

interface FetchError {
  code: number;
  message: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  error_chain?: FetchError[];
}

interface FetchResult<ResponseType = unknown> {
  success: boolean;
  result: ResponseType;
  errors: FetchError[];
  messages: string[];
  // eslint-disable-next-line @typescript-eslint/naming-convention
  result_info?: unknown;
}
