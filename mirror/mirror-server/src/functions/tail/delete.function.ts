import type {Auth} from 'firebase-admin/auth';
import type {Firestore} from 'firebase-admin/firestore';
import {
  DeleteTailResponse,
  deleteTailRequestSchema,
  deleteTailResponseSchema,
} from 'mirror-protocol/src/tail.js';
import {cfFetch} from '../../cloudflare/cf-fetch.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {defineSecret, defineString} from 'firebase-functions/params';

// This is the API token for reflect-server.net
// https://dash.cloudflare.com/085f6d8eb08e5b23debfb08b21bda1eb/
const cloudflareApiToken = defineSecret('CLOUDFLARE_API_TOKEN');

const cloudflareAccountId = defineString('CLOUDFLARE_ACCOUNT_ID');

const deleteTail = (firestore: Firestore, _auth: Auth) =>
  validateSchema(deleteTailRequestSchema, deleteTailResponseSchema)
    .validate(userAuthorization())
    .validate(appAuthorization(firestore))
    .handle(async (deleteTailRequest, _context) => {
      const {tailID, env} = deleteTailRequest;
      // TODO(cesar) Get this from `getApp`
      const cfWorkerName = 'arv-cli-test-1';
      const apiToken =
        cloudflareApiToken.value() ||
        '7egl0VDDRceLm853K9YMrGF_DYn4BCnt4R8NvZjz';
      const accountID = cloudflareAccountId.value();
      const {success} = await cfFetch<DeleteTailResponse>(
        apiToken,
        makeDeleteTailUrl(accountID, cfWorkerName, tailID, env),
        {
          method: 'DELETE',
        },
      );
      return {success};
    });

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
  env: string | undefined,
): string {
  return env
    ? `/accounts/${accountId}/workers/services/${workerName}/environments/${env}/tails/${tailId}`
    : `/accounts/${accountId}/workers/scripts/${workerName}/tails/${tailId}`;
}

export {deleteTail as delete};
