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
import {must} from 'shared/src/must.js';
import {
  membershipDataConverter,
  teamInvitePath,
  teamMembershipPath,
} from 'mirror-schema/src/membership.js';

export function ensure(
  firestore: Firestore,
  auth: Auth,
): AsyncCallable<EnsureUserRequest, EnsureUserResponse> {
  return withSchema(
    ensureUserRequestSchema,
    ensureUserResponseSchema,
    withAuthorization(async (ensureUserRequest, context) => {
      const {userID} = ensureUserRequest.requester;

      const user = await auth.getUser(userID);
      if (!user.email) {
        throw new HttpsError(
          'failed-precondition',
          'User must have an email address',
        );
      }
      const {email} = user;
      const userDocRef = firestore
        .doc(userPath(userID))
        .withConverter(userDataConverter);

      await firestore.runTransaction(async txn => {
        const userDoc = await txn.get(userDocRef);
        if (!userDoc.exists) {
          // A new User is not part of any teams. They are associated with a Team:
          // - When creating a new App (i.e `reflect init`).
          // - When invited to join a Team (future feature).
          txn.create(userDocRef, {email, roles: {}});
        } else {
          const user = must(userDoc.data());
          if (user.email !== email) {
            logger.info(
              `Updating email of ${userID} from ${user.email} to ${email}`,
            );

            txn.update(userDocRef, {email});

            for (const teamID of Object.keys(user.roles)) {
              const membershipDocRef = firestore
                .doc(teamMembershipPath(teamID, userID))
                .withConverter(membershipDataConverter);
              txn.update(membershipDocRef, {email});
            }
            for (const teamID of Object.keys(user.invites ?? {})) {
              const inviteDocRef = firestore
                .doc(teamInvitePath(teamID, userID))
                .withConverter(membershipDataConverter);
              txn.update(inviteDocRef, {email});
            }
          }
        }
      });
      const customToken = await auth.createCustomToken(context.auth.uid);
      return {
        customToken,
        success: true,
      };
    }),
  );
}
