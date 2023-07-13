import type {Firestore} from 'firebase-admin/firestore';
import type {Auth} from 'firebase-admin/auth';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  EnsureUserRequest,
  EnsureUserResponse,
  ensureUserRequestSchema,
  ensureUserResponseSchema,
} from 'mirror-protocol/src/user.js';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import {withAuthorization} from '../validators/auth.js';
import {withSchema} from '../validators/schema.js';
import type {AsyncCallable} from '../validators/types.js';
import {logger} from 'firebase-functions';

export function ensure(
  firestore: Firestore,
  auth: Auth,
): AsyncCallable<EnsureUserRequest, EnsureUserResponse> {
  return withSchema(
    ensureUserRequestSchema,
    ensureUserResponseSchema,
    withAuthorization(async (ensureUserRequest, context) => {
      const {userID} = ensureUserRequest.requester;

      const userDocRef = firestore
        .doc(userPath(userID))
        .withConverter(userDataConverter);

      await firestore.runTransaction(async txn => {
        const email = context.auth?.token?.email;
        if (!email) {
          throw new HttpsError(
            'failed-precondition',
            'Authenticated user must have an email address',
          );
        }
        const userDoc = await txn.get(userDocRef);
        if (userDoc.exists) {
          const user = userDoc.data();
          if (user?.email !== email) {
            // TODO: Update userDoc and denormalized email addresses (in Team Memberships) to the
            //       new email address. It's possible that this may happen if the user changes their
            //       email address in Github.
            logger.warn(
              `Authenticated email ${email} does not match user doc email ${user?.email}`,
            );
          }
          return;
        }
        // A new User is not part of any teams. They are associated with a Team:
        // - When creating a new App (i.e `reflect init`).
        // - When invited to join a Team (future feature).
        txn.create(userDocRef, {email, roles: {}});
      });
      const customToken = await auth.createCustomToken(context.auth.uid);
      return {
        customToken,
        success: true,
      };
    }),
  );
}
