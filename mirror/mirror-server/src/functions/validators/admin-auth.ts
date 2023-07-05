import {HttpsError} from 'firebase-functions/v2/https';
import type {BaseRequest} from 'mirror-protocol/src/base.js';
import {withAuthorization} from './auth.js';
import type {AsyncHandler} from './types.js';

export function withAdminAuthorization<Request extends BaseRequest, Response>(
  handler: AsyncHandler<Request, Response>,
): AsyncHandler<Request, Response> {
  // eslint-disable-next-line require-await
  return withAuthorization(async (payload, context) => {
    if (context.auth === undefined || !isAdminUserID(context.auth.uid)) {
      throw new HttpsError(
        'permission-denied',
        'authenticated user is not authorized to make this request',
      );
    }

    return handler(payload, context);
  });
}

function isAdminUserID(userID: string): boolean {
  return [
    'JqAWJV0QqAZ1DAMbwAtqyZtUuJ92', // arv
    // Add more
  ].includes(userID);
}
