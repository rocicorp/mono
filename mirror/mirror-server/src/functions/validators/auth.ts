import {CallableRequest, HttpsError} from 'firebase-functions/v2/https';
import type {BaseRequest} from 'mirror-protocol/src/base.js';
import type {BaseAppRequest} from 'mirror-protocol/src/app.js';
import type {
  AsyncHandler,
  AsyncHandlerWithAuth,
  AsyncAppHandler,
  CallableRequestWithAuth,
} from './types.js';
import type {Firestore} from 'firebase-admin/firestore';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import {appDataConverter, appPath} from 'mirror-schema/src/app.js';
import {must} from 'shared/src/must.js';
import {logger} from 'firebase-functions';
import {SHORT_TO_LONG_ROLE, type Role} from 'mirror-schema/src/membership.js';

export function withAuthorization<Request extends BaseRequest, Response>(
  handler: AsyncHandlerWithAuth<Request, Response>,
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
    return handler(payload, context as CallableRequestWithAuth<Request>);
  };
}

export function withAppAuthorization<Request extends BaseAppRequest, Response>(
  firestore: Firestore,
  handler: AsyncAppHandler<Request, Response>,
  allowedRoles: Role[] = ['admin', 'member'],
): AsyncHandler<Request, Response> {
  return withAuthorization(
    async (payload: Request, context: CallableRequestWithAuth<Request>) => {
      const {userID} = payload.requester;
      const userDocRef = firestore
        .doc(userPath(userID))
        .withConverter(userDataConverter);
      const {appID} = payload;
      const appDocRef = firestore
        .doc(appPath(appID))
        .withConverter(appDataConverter);

      const authorized = await firestore.runTransaction(
        async txn => {
          const [userDoc, appDoc] = await Promise.all([
            txn.get(userDocRef),
            txn.get(appDocRef),
          ]);
          if (!userDoc.exists) {
            throw new HttpsError(
              'failed-precondition',
              `User ${userID} has not been initialized`,
            );
          }
          if (!appDoc.exists) {
            throw new HttpsError('not-found', `App ${appID} does not exist`);
          }
          const user = must(userDoc.data());
          const app = must(appDoc.data());
          const {teamID} = app;
          const role = SHORT_TO_LONG_ROLE[user.roles[teamID]];
          if (allowedRoles.indexOf(role) < 0) {
            throw new HttpsError(
              'permission-denied',
              `User ${userID} has insufficient permissions for App ${appID}`,
            );
          }
          logger.info(
            `User ${userID} has role ${role} in team ${teamID} of app ${appID}`,
          );
          return {app, user, role};
        },
        {readOnly: true},
      );

      return handler(payload, {...context, authorized});
    },
  );
}
