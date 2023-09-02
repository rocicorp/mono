import type {Config} from './config.js';
import {cfFetch} from './cf-fetch.js';
import {logger} from 'firebase-functions';

// https://github.com/cloudflare/workers-sdk/blob/e9fae5586c14eeae8bb44e0dcf940052635575b4/packages/wrangler/src/delete.ts#L93
export async function deleteScript(config: Config): Promise<void> {
  const {apiToken, accountID, scriptName} = config;
  const resource = `/accounts/${accountID}/workers/scripts/${scriptName}`;

  try {
    await cfFetch(
      apiToken,
      resource,
      {method: 'DELETE'},
      new URLSearchParams({force: 'true'}),
    );
  } catch (e) {
    // Error returned by Cloudflare when the script is not found (already deleted)
    // {
    //   "code": 10007,
    //   "message": "workers.api.error.script_not_found"
    // }
    // an attached to the ParseError in throwFetchError().
    if ((e as unknown as {code?: number}).code === 10007) {
      // Log a warning but otherwise consider it a success.
      logger.warn(`Script ${scriptName} was not found in Cloudflare`, e);
    } else {
      throw e;
    }
  }
  logger.info(`Deleted script ${scriptName}`);
}
