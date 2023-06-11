import {CallableRequest, HttpsError} from 'firebase-functions/v2/https';
import type {BaseRequest} from 'mirror-protocol/base.js';
import type {AsyncHandler} from './types.js';

export function withAuthorization<Request extends BaseRequest, Response>(
  handler: AsyncHandler<Request, Response>,
): AsyncHandler<Request, Response> {
  // eslint-disable-next-line require-await
  return async (payload: Request, context: CallableRequest<Request>) => {
    if (context.auth?.uid === undefined) {
      throw new HttpsError('unauthenticated', 'missing authentication');
    }
    if (context.auth.uid !== payload.requester.userID) {
      // TODO: Add support for admin access / impersonation.
      throw new HttpsError(
        'permission-denied',
        'authenticated user is not authorized to make this request',
      );
    }
    return handler(payload, context);
  };
}
