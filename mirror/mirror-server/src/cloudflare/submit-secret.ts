// Original code at https://github.com/cloudflare/workers-sdk/blob/a728876e607635081cd1ed00d06b7af86e7efd49/packages/wrangler/src/secret/index.ts#L69

import type {Script} from 'cloudflare-api/src/scripts.js';

export function submitSecret(script: Script, key: string, secretValue: string) {
  return script.putSecret({
    name: key,
    text: secretValue,
    type: 'secret_text',
  });
}
