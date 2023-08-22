import type {Firestore} from 'firebase-admin/firestore';
import {defineString} from 'firebase-functions/params';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  createRequestSchema,
  createResponseSchema,
} from 'mirror-protocol/src/app.js';
import {
  App,
  appDataConverter,
  appNameIndexDataConverter,
  appNameIndexPath,
  appPath,
} from 'mirror-schema/src/app.js';
import {
  Membership,
  membershipDataConverter,
  teamMembershipPath,
} from 'mirror-schema/src/membership.js';
import {Team, teamDataConverter, teamPath} from 'mirror-schema/src/team.js';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import assert from 'node:assert';
import {
  newAppID,
  newAppIDAsNumber,
  newAppScriptName,
  newTeamID,
} from 'shared/src/mirror/ids.js';
import {must} from 'shared/src/must.js';
import {userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {defaultOptions} from 'mirror-schema/src/deployment.js';

const cloudflareAccountId = defineString('CLOUDFLARE_ACCOUNT_ID');

export const DEFAULT_MAX_APPS = null;

export const create = (firestore: Firestore) =>
  validateSchema(createRequestSchema, createResponseSchema)
    .validate(userAuthorization())
    .handle((request, context) => {
      const {userID} = context;
      const {serverReleaseChannel} = request;

      const userDocRef = firestore
        .doc(userPath(userID))
        .withConverter(userDataConverter);

      return firestore.runTransaction(async txn => {
        const userDoc = await txn.get(userDocRef);
        if (!userDoc.exists) {
          throw new HttpsError('not-found', `User ${userID} does not exist`);
        }

        const user = must(userDoc.data());
        const {email} = user;

        const teamIDs = Object.keys(user.roles);
        let teamID: string;

        const createNewTeam = teamIDs.length === 0;
        if (createNewTeam) {
          // User is not a member of any team. Create a new team.
          teamID = newTeamID();
        } else if (teamIDs.length === 1) {
          // User is a member of one team. Use that team.
          teamID = teamIDs[0];
        } else {
          throw new HttpsError(
            'internal',
            'User is part of multiple teams, but only one team is supported at this time',
          );
        }

        const teamDocRef = firestore
          .doc(teamPath(teamID))
          .withConverter(teamDataConverter);
        const teamDoc = await txn.get(teamDocRef);

        let team: Team;
        let membership: Membership | undefined;

        if (createNewTeam) {
          if (teamDoc.exists) {
            throw new HttpsError('already-exists', 'Team already exists');
          }
          team = {
            name: '',
            defaultCfID: cloudflareAccountId.value(),
            numAdmins: 1,
            numMembers: 0,
            numInvites: 0,
            numApps: 1,
            maxApps: DEFAULT_MAX_APPS,
          };
          user.roles[teamID] = 'admin';
          membership = {email, role: 'admin'};
        } else {
          if (!teamDoc.exists) {
            throw new HttpsError('not-found', 'Team does not exist');
          }
          team = must(teamDoc.data());
          // Check app limits
          if (team.maxApps !== null && team.numApps >= team.maxApps) {
            throw new HttpsError(
              'resource-exhausted',
              'Team has too many apps',
            );
          }
          team.numApps++;
          // No need to change admins or members. User is already part of team.
          // No need to create a membership. User is already a member of team.
        }

        const membershipDocRef = firestore
          .doc(teamMembershipPath(teamID, userID))
          .withConverter(membershipDataConverter);
        const membershipDoc = await txn.get(membershipDocRef);
        if (!createNewTeam) {
          if (!membershipDoc.exists) {
            throw new HttpsError('internal', 'Team membership should exist');
          }
        } else {
          if (membershipDoc.exists) {
            throw new HttpsError('internal', 'Team membership already exists');
          }
        }

        const appIDNumber = newAppIDAsNumber();
        const appID = newAppID(appIDNumber);
        const scriptName = newAppScriptName(appIDNumber);

        // TODO(arv): Ensure that Cloudflare is OK with this script name?
        const appDocRef = firestore
          .doc(appPath(appID))
          .withConverter(appDataConverter);
        const appNameDocRef = firestore
          .doc(appNameIndexPath(scriptName))
          .withConverter(appNameIndexDataConverter);
        const appDoc = await txn.get(appDocRef);
        if (appDoc.exists) {
          throw new HttpsError('already-exists', 'App already exists');
        }

        const app: App = {
          name: scriptName,
          teamID,
          cfID: cloudflareAccountId.value(),
          cfScriptName: scriptName,
          serverReleaseChannel,
          deploymentOptions: defaultOptions(),
        };

        if (createNewTeam) {
          txn.create(teamDocRef, team);
          txn.update(userDocRef, user);
          assert(membership);
          txn.create(membershipDocRef, membership);
        } else {
          txn.update(teamDocRef, team);
        }
        txn.create(appDocRef, app);
        txn.create(appNameDocRef, {appID});

        return {appID, name: scriptName, success: true};
      });
    });
