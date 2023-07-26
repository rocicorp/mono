import {CallableRequest, HttpsError} from 'firebase-functions/v2/https';
import type {BaseRequest} from 'mirror-protocol/src/base.js';
import type {BaseAppRequest} from 'mirror-protocol/src/app.js';
import type {
  RequestContextValidator,
  UserAuthorization,
  AppAuthorization,
} from './types.js';
import type {Firestore} from 'firebase-admin/firestore';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import {appDataConverter, appPath} from 'mirror-schema/src/app.js';
import {must} from 'shared/src/must.js';
import {logger} from 'firebase-functions';
import type {Role} from 'mirror-schema/src/membership.js';
import {assert} from 'shared/src/asserts.js';

/**
 * Validator that checks the original authentication against the
 * requester userID and initializes a {@link UserAuthorization} context.
 */
export function userAuthorization<
  Request extends BaseRequest,
>(): RequestContextValidator<
  Request,
  CallableRequest<Request>,
  UserAuthorization
> {
  return (request, context) => {
    if (context.auth?.uid === undefined) {
      throw new HttpsError('unauthenticated', 'missing authentication');
    }
    if (context.auth.uid !== request.requester.userID) {
      // TODO: Add support for admin access / impersonation.
      throw new HttpsError(
        'permission-denied',
        'authenticated user is not authorized to make this request',
      );
    }
    // From here we return a brand new context in order to avoid
    // code from erroneously referencing the authenticated user
    // (i.e. context.auth.uid); subsequent logic should be based
    // on the requester.userID.
    return {userID: request.requester.userID};
  };
}

/**
 * Validates that the authorized user has privileges to modify the
 * app associated with the request.
 */
export function appAuthorization<
  Request extends BaseAppRequest,
  Context extends UserAuthorization,
>(firestore: Firestore, allowedRoles: Role[] = ['admin', 'member']) {
  assert(allowedRoles.length > 0, 'allowedRoles must be non-empty');
  return async (request: Request, context: Context) => {
    const {userID} = context;
    const userDocRef = firestore
      .doc(userPath(userID))
      .withConverter(userDataConverter);
    const {appID} = request;
    const appDocRef = firestore
      .doc(appPath(appID))
      .withConverter(appDataConverter);

    const authorization: AppAuthorization = await firestore.runTransaction(
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
        const role = user.roles[teamID];
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
    return {...context, ...authorization};
  };
}
