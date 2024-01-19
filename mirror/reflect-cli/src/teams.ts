import {Firestore, doc, getDoc} from 'firebase/firestore';
import type {Role} from 'mirror-schema/src/external/membership.js';
import {
  userPath,
  userViewDataConverter,
} from 'mirror-schema/src/external/user.js';
import {must} from 'shared/src/must.js';

export async function getSingleTeam(
  firestore: Firestore,
  userID: string,
  restrictToRole?: Role,
): Promise<string> {
  const teams = await getTeams(firestore, userID, restrictToRole);
  switch (teams.size) {
    case 0:
      throw new Error(
        `You are not ${
          restrictToRole === 'admin' ? 'an admin' : 'a member'
        } of any teams`,
      );
    case 1:
      return teams.keys().next().value;
    default:
      throw new Error(
        'This version of @rocicorp/reflect does not support multiple teams. Please update to the latest version.',
      );
  }
}

async function getTeams(
  firestore: Firestore,
  userID: string,
  restrictToRole?: Role,
): Promise<Map<string, Role>> {
  const userDoc = await getDoc(
    doc(firestore, userPath(userID)).withConverter(userViewDataConverter),
  );
  if (!userDoc.exists()) {
    throw new Error('UserDoc does not exist.');
  }
  const {roles} = must(userDoc.data());
  return new Map(
    Object.entries(roles).filter(
      ([_, role]) => role === restrictToRole ?? role,
    ),
  );
}
