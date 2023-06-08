import {getFirestore} from 'firebase-admin/firestore';
import {
  EnsureUserRequest,
  EnsureUserResponse,
  ensureUserRequestSchema,
  ensureUserResponseSchema,
} from 'mirror-protocol/user.js';
import {userPath} from 'mirror-schema/user.js';
import {withAuthorization} from '../validators/auth';
import {withSchema} from '../validators/schema';

export const ensure = withSchema(
  ensureUserRequestSchema,
  ensureUserResponseSchema,
  withAuthorization(ensureHandler),
);

async function ensureHandler(
  payload: EnsureUserRequest,
): Promise<EnsureUserResponse> {
  const firestore = getFirestore();
  const {userID} = payload.requester;
  const userDocRef = firestore.doc(userPath(userID));
  await firestore.runTransaction(async txn => {
    const userDoc = await txn.get(userDocRef);
    if (userDoc.exists) {
      return; // TODO: Consider validating Team membership.
    }
    // TODO: Create the User doc with a new team (uuid.v4() TeamID) with
    // the user as an 'admin' member.
    console.info('Would create the user here.');
  });
  return {success: true};
}
