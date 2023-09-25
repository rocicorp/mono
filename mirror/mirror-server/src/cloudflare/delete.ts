import {logger} from 'firebase-functions';
import type {Script} from 'cloudflare-api/src/scripts.js';
import {Errors, FetchResultError} from 'cloudflare-api/src/fetch.js';

// https://github.com/cloudflare/workers-sdk/blob/e9fae5586c14eeae8bb44e0dcf940052635575b4/packages/wrangler/src/delete.ts#L93
export async function deleteScript(script: Script): Promise<void> {
  try {
    await script.delete(new URLSearchParams({force: 'true'}));
  } catch (e) {
    FetchResultError.throwIfCodeIsNot(
      e,
      Errors.ScriptNotFound,
      Errors.CouldNotRouteToScript,
    );
  }
  logger.info(`Deleted script ${script.id}`);
}
