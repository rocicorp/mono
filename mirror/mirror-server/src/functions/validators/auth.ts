import type {Auth} from 'firebase-admin/auth';
import {FieldValue, type Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {HttpsError} from 'firebase-functions/v2/https';
import type {AuthData} from 'firebase-functions/v2/tasks';
import type {BaseAppRequest} from 'mirror-protocol/src/app.js';
import type {BaseRequest} from 'mirror-protocol/src/base.js';
import {
  appKeyDataConverter,
  type RequiredPermission,
} from 'mirror-schema/src/app-key.js';
import {appDataConverter, appPath} from 'mirror-schema/src/app.js';
import type {Role} from 'mirror-schema/src/membership.js';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import {assert} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import type {HttpsRequestContext} from './https.js';
import type {
  AppAuthorization,
  RequestContextValidator,
  UserAuthorization,
  UserOrKeyAuthorization,
} from './types.js';

// The subset of CallableRequest fields applicable to `userAuthorization`.
interface AuthContext {
  auth?: AuthData;
}

const BEARER_PREFIX = 'bearer ';

/**
 * Creates an `AuthContext` from an `onRequest()` HttpsRequestContext by parsing
 * and verifying the `Authorization: Bearer` request header. This bridges the
 * API for https requests to callable requests.
 */
export function tokenAuthentication<
  Request,
  Context extends HttpsRequestContext,
>(
  auth: Auth,
): RequestContextValidator<Request, Context, Context & AuthContext> {
  return async (_, context) => {
    const authorization =
      context.request.headers['Authorization'] ??
      context.request.headers['authorization'];
    if (typeof authorization !== 'string') {
      throw new HttpsError('unauthenticated', 'Invalid Authorization header');
    }
    if (!authorization.toLowerCase().startsWith(BEARER_PREFIX)) {
      throw new HttpsError(
        'unimplemented',
        'Only Bearer Authorization is supported',
      );
    }
    const token = authorization.substring(BEARER_PREFIX.length);
    const decodedIdToken = await auth.verifyIdToken(token);
    return {...context, auth: {uid: decodedIdToken.uid, token: decodedIdToken}};
  };
}

/**
 * Validator that checks the original authentication against the
 * requester userID and initializes a {@link UserOrKeyAuthorization} context.
 */
export function userOrKeyAuthorization<
  Request extends BaseRequest,
  Context extends AuthContext,
>(): RequestContextValidator<
  Request,
  Context,
  // Remove the 'auth' field from the OutputContext to prevent
  // downstream code from erroneously referencing the authenticated
  // user (i.e. context.auth.uid); subsequent logic should be based
  // on the requester.userID.
  Omit<Context, 'auth'> & UserOrKeyAuthorization
> {
  return userAuthorizationImpl(true);
}

/**
 * Validator that checks the original authentication against the
 * requester userID and initializes a {@link UserAuthorization} context.
 */
export function userAuthorization<
  Request extends BaseRequest,
  Context extends AuthContext,
>(): RequestContextValidator<
  Request,
  Context,
  // Remove the 'auth' field from the OutputContext to prevent
  // downstream code from erroneously referencing the authenticated
  // user (i.e. context.auth.uid); subsequent logic should be based
  // on the requester.userID.
  Omit<Context, 'auth'> & UserAuthorization
> {
  return userAuthorizationImpl(false);
}

/**
 * Internal implementation for `userOrKeyAuthorization` and `userAuthorization`.
 * The two are exported as separate methods to facilitate type-safety of the
 * OutputContext. (Namely, the type system requires that `appOrKeyAuthorization()`
 * be preceded by `userOrKeyAuthorization()` because it requires a
 * `UserOrKeyAuthorization` InputContext.)
 */
function userAuthorizationImpl<
  Request extends BaseRequest,
  Context extends AuthContext,
>(
  allowKeys: boolean,
): RequestContextValidator<
  Request,
  Context,
  Omit<Context, 'auth'> & UserOrKeyAuthorization
> {
  return (request, context) => {
    if (context.auth?.uid === undefined) {
      throw new HttpsError('unauthenticated', 'missing authentication');
    }
    if (!allowKeys && context.auth.uid.includes('/')) {
      throw new HttpsError(
        'permission-denied',
        'Keys are not authorized for this action',
      );
    }
    if (context.auth.uid !== request.requester.userID) {
      // Check custom claims for temporary super powers.
      const superUntil = context.auth.token?.superUntil;
      if (typeof superUntil === 'number' && superUntil >= Date.now()) {
        logger.info(
          `${context.auth.uid} (${context.auth.token.email}) impersonating ${request.requester.userID}`,
        );
      } else {
        throw new HttpsError(
          'permission-denied',
          'authenticated user is not authorized to make this request',
        );
      }
    }
    return {
      ...context,
      userID: request.requester.userID,
      isKeyAuth: context.auth.uid.includes('/'),
    };
  };
}

export function appOrKeyAuthorization<
  Request extends BaseAppRequest,
  Context extends UserOrKeyAuthorization,
>(
  firestore: Firestore,
  keyPermission: RequiredPermission,
  allowedRoles: Role[] = ['admin', 'member'],
): RequestContextValidator<Request, Context, Context & AppAuthorization> {
  const nonKeyAppAuthorization = appAuthorization<Request, Context>(
    firestore,
    allowedRoles,
  );

  return async (request: Request, context: Context) => {
    const {isKeyAuth} = context;
    if (!isKeyAuth) {
      return nonKeyAppAuthorization(request, context);
    }
    const {userID: keyPath} = context;
    const appKeyDocRef = firestore
      .doc(keyPath)
      .withConverter(appKeyDataConverter);
    const {appID} = request;
    const appDocRef = firestore
      .doc(appPath(appID))
      .withConverter(appDataConverter);

    if (appKeyDocRef.parent?.parent?.path !== appDocRef.path) {
      throw new HttpsError(
        'permission-denied',
        `Key "${appKeyDocRef.id}" is not authorized for app ${appID}`,
      );
    }

    const authorization: AppAuthorization = await firestore.runTransaction(
      async txn => {
        const [appKeyDoc, appDoc] = await Promise.all([
          txn.get(appKeyDocRef),
          txn.get(appDocRef),
        ]);
        if (!appKeyDoc.exists) {
          throw new HttpsError(
            'permission-denied',
            `Key "${appKeyDoc.id}" has been deleted`,
          );
        }
        if (!appDoc.exists) {
          throw new HttpsError('not-found', `App ${appID} does not exist`);
        }
        const appKey = must(appKeyDoc.data());
        const app = must(appDoc.data());
        if (!appKey.permissions[keyPermission]) {
          throw new HttpsError(
            'permission-denied',
            `Key "${appKeyDoc.id}" has not been granted "${keyPermission}" permission`,
          );
        }
        logger.info(
          `Key "${keyPath}" authorized with ${keyPermission} permission`,
        );
        txn.update(appKeyDocRef, {lastUsed: FieldValue.serverTimestamp()});
        return {app};
      },
      // TODO(darick): Add a mechanism for initiating writes (like the `lastUsed` timestamp update)
      // in the background so as not to delay request processing. Then this Transaction can be readOnly.
      // {readOnly: true},
    );
    return {...context, ...authorization};
  };
}

/**
 * Validates that the authorized user has privileges to modify the
 * app associated with the request.
 */
export function appAuthorization<
  Request extends BaseAppRequest,
  Context extends UserAuthorization,
>(
  firestore: Firestore,
  allowedRoles: Role[] = ['admin', 'member'],
): RequestContextValidator<Request, Context, Context & AppAuthorization> {
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
        return {app};
      },
      {readOnly: true},
    );
    return {...context, ...authorization};
  };
}
