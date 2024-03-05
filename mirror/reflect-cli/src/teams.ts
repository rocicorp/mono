import {Firestore, doc, getDoc} from 'firebase/firestore';
import type {Role} from 'mirror-schema/src/external/membership.js';
import {
  userPath,
  userViewDataConverter,
} from 'mirror-schema/src/external/user.js';
import {must} from 'shared/src/must.js';
import {ensureTeamID} from './app-config.js';
import {ErrorWithSeverity} from './error.js';
import type {AuthContext} from './handler.js';

export async function getSingleTeam(
  firestore: Firestore,
  authContext: AuthContext,
  restrictToRole?: Role,
): Promise<string> {
  const teams = await getTeams(
    firestore,
    authContext.user.userID,
    restrictToRole,
  );
  switch (teams.size) {
    case 0:
      // User has no apps (and thus no team). Create a team for the user.
      return ensureTeamID(authContext);
    case 1:
      return teams.keys().next().value;
    default:
      throw new ErrorWithSeverity(
        'This version of @rocicorp/reflect does not support multiple teams. Please update to the latest version.',
        'WARNING',
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
