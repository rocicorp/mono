import {logger} from 'firebase-functions';
import type {GlobalScript} from 'cloudflare-api/src/scripts.js';

// Original file was here:
// https://github.com/cloudflare/workers-sdk/blob/a728876e607635081cd1ed00d06b7af86e7efd49/packages/wrangler/src/deploy/deploy.ts#L170
//
// The original file allowed an interactive mode if the terminal was a TTY. We
// remove that to simplify the code and it is always running in non-interactive
// mode. Therefore existing origins / dns records are not indicative of errors,
// so we aggressively update rather than aggressively fail

export function publishCustomDomains(script: GlobalScript, hostname: string) {
  /* eslint-disable @typescript-eslint/naming-convention */
  const body = {
    override_scope: true,
    override_existing_origin: true,
    override_existing_dns_record: true,
    origins: [{hostname}],
  };
  /* eslint-enable @typescript-eslint/naming-convention */

  logger.log('Setting up custom domain:', hostname);

  // deploy to domains
  return script.setCustomDomains(body);
}
