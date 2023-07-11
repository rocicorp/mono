// Original code at https://github.com/cloudflare/workers-sdk/blob/a728876e607635081cd1ed00d06b7af86e7efd49/packages/wrangler/src/secret/index.ts#L69

import {cfFetch} from './cf-fetch.js';
import type {Config} from './config.js';

export function submitSecret(config: Config, key: string, secretValue: string) {
  const {accountID, apiToken, scriptName, env} = config;
  const url = !env
    ? `/accounts/${accountID}/workers/scripts/${scriptName}/secrets`
    : `/accounts/${accountID}/workers/services/${scriptName}/environments/${env}/secrets`;

  return cfFetch(apiToken, url, {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      name: key,
      text: secretValue,
      type: 'secret_text',
    }),
  });
}
