import type {Config} from './config.js';
import {logger} from 'firebase-functions';
import {GlobalScript} from 'cloudflare-api/src/scripts.js';
import {Errors, FetchResultError} from 'cloudflare-api/src/fetch.js';

// https://github.com/cloudflare/workers-sdk/blob/e9fae5586c14eeae8bb44e0dcf940052635575b4/packages/wrangler/src/delete.ts#L93
export async function deleteScript(config: Config): Promise<void> {
  const {apiToken, accountID, scriptName} = config;
  const script = new GlobalScript(apiToken, accountID, scriptName);

  try {
    await script.delete(new URLSearchParams({force: 'true'}));
  } catch (e) {
    FetchResultError.throwIfCodeIsNot(
      e,
      Errors.ScriptNotFound,
      Errors.CouldNotRouteToScript,
    );
  }
  logger.info(`Deleted script ${scriptName}`);
}
