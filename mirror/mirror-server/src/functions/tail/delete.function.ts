import type { Auth } from 'firebase-admin/auth';
import type { Firestore } from 'firebase-admin/firestore';
import {
  deleteTailRequestSchema,
  deleteTailResponseSchema,
} from 'mirror-protocol/src/tail.js';
import { cfFetch } from '../../cloudflare/cf-fetch.js';
import { appAuthorization, userAuthorization } from '../validators/auth.js';
import { validateSchema } from '../validators/schema.js';

const deleteTail = (firestore: Firestore, auth: Auth) =>
  validateSchema(deleteTailRequestSchema, deleteTailResponseSchema)
    .validate(userAuthorization())
    .validate(appAuthorization(firestore))
    .handle(async (deleteTailRequest, context) => {
      const {appID} = deleteTailRequest;
      const {userID, app} = context;

      const apiToken 
      await cfFetch(apiToken, makeDeleteTailUrl(accountId, workerName, tailId, env), {
        method: 'DELETE',
      });
    };

/**
 * Generate a URL that, when `cfetch`ed, deletes a tail
 *
 * https://api.cloudflare.com/#worker-tail-logs-delete-tail
 *
 * @param accountId the account ID associated with the worker we're tailing
 * @param workerName the name of the worker we're tailing
 * @param tailId the ID of the tail we want to delete
 * @returns a `cfetch`-ready URL for deleting a tail
 */
function makeDeleteTailUrl(
	accountId: string,
	workerName: string,
	tailId: string,
	env: string | undefined
): string {
	return env
		? `/accounts/${accountId}/workers/services/${workerName}/environments/${env}/tails/${tailId}`
		: `/accounts/${accountId}/workers/scripts/${workerName}/tails/${tailId}`;
}

export { deleteTail as delete };
