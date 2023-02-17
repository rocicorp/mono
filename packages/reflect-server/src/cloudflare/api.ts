import type {JSONValue} from 'replicache';

// https://api.cloudflare.com/#durable-objects-namespace-list-namespaces
export function listDONamespaces(accountID: string, apiToken: string) {
  return jsonFetchCFAPI(
    `https://api.cloudflare.com/client/v4/accounts/${accountID}/workers/durable_objects/namespaces`,
    apiToken,
  );
}

export type DONamespace = {
  id: string;
  name: string;
  script: string;
  environment?: string;
  class: string;
};

type APIResponse = {
  success: boolean;
  result: JSONValue;
};

export type DONamespaces = Array<DONamespace>;

// https://api.cloudflare.com/#durable-objects-namespace-list-namespaces
// Note: Does not handle cursor pagination. Sets limit to 10,000.
// Note: This can take a while.
export function listDOInstances(
  accountID: string,
  apiToken: string,
  namespaceID: string,
) {
  return jsonFetchCFAPI(
    `https://api.cloudflare.com/client/v4/accounts/${accountID}/workers/durable_objects/namespaces/${namespaceID}/objects?limit=10000`,
    apiToken,
  );
}

export type DOInstance = {
  id: string;
  hasStoredData: boolean;
};

export type DOInstances = Array<DOInstance>;

async function jsonFetchCFAPI(url: string, apiToken: string) {
  const resp = await fetch(url, {
    headers: {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      Authorization: `Bearer ${apiToken}`,
    },
  });
  const body = (await resp.json()) as APIResponse;
  if (!body.success) {
    throw new Error(`API error: ${JSON.stringify(body)}`);
  }
  return body.result;
}
