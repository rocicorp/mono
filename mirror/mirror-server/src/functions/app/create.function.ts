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
  appPath,
  isValidAppName,
} from 'mirror-schema/src/app.js';
import {
  teamDataConverter,
  teamPath,
  appNameIndexPath,
  appNameIndexDataConverter,
} from 'mirror-schema/src/team.js';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import {
  newAppID,
  newAppIDAsNumber,
  newAppScriptName,
} from 'shared/src/mirror/ids.js';
import {must} from 'shared/src/must.js';
import {userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {defaultOptions} from 'mirror-schema/src/deployment.js';

const cloudflareAccountId = defineString('CLOUDFLARE_ACCOUNT_ID');

// TODO(darick): Reduce this once (or make it configurable by stack)
// once we've cleaned up all of the throwaway apps in staging.
export const DEFAULT_MAX_APPS = 100;

export const create = (firestore: Firestore) =>
  validateSchema(createRequestSchema, createResponseSchema)
    .validate(userAuthorization())
    .handle((request, context) => {
      const {userID} = context;
      const {teamID, serverReleaseChannel, name: appName} = request;

      if (!teamID || !appName) {
        throw new HttpsError(
          'invalid-argument',
          'Please update to the latest release of @rocicorp/reflect',
        );
      }

      if (appName !== undefined && !isValidAppName(appName)) {
        throw new HttpsError(
          'invalid-argument',
          `Invalid App Name "${appName}". Names must be lowercased alphanumeric, starting with a letter and not ending with a hyphen.`,
        );
      }

      const userDocRef = firestore
        .doc(userPath(userID))
        .withConverter(userDataConverter);
      const teamDocRef = firestore
        .doc(teamPath(teamID))
        .withConverter(teamDataConverter);

      return firestore.runTransaction(async txn => {
        const userDoc = await txn.get(userDocRef);
        if (!userDoc.exists) {
          throw new HttpsError('not-found', `User ${userID} does not exist`);
        }

        const user = must(userDoc.data());
        const role = user.roles[teamID];
        if (role !== 'admin') {
          throw new HttpsError(
            'permission-denied',
            `User ${userID} does not have permission to create new apps for team ${teamID}`,
          );
        }

        const teamDoc = await txn.get(teamDocRef);
        if (!teamDoc.exists) {
          throw new HttpsError('not-found', `Team ${teamID} does not exist`);
        }
        // Check app limits
        const team = must(teamDoc.data());
        if (team.numApps >= (team.maxApps ?? DEFAULT_MAX_APPS)) {
          throw new HttpsError('resource-exhausted', 'Team has too many apps');
        }

        const appIDNumber = newAppIDAsNumber();
        const appID = newAppID(appIDNumber);
        const scriptName = newAppScriptName(appIDNumber);

        const appDocRef = firestore
          .doc(appPath(appID))
          .withConverter(appDataConverter);
        const appNameDocRef = firestore
          .doc(appNameIndexPath(teamID, appName))
          .withConverter(appNameIndexDataConverter);

        const app: App = {
          name: appName,
          teamID,
          teamSubdomain: team.subdomain,
          cfID: cloudflareAccountId.value(),
          cfScriptName: scriptName,
          serverReleaseChannel,
          deploymentOptions: defaultOptions(),
        };

        txn.update(teamDocRef, {numApps: team.numApps + 1});
        txn.create(appDocRef, app);
        txn.create(appNameDocRef, {appID});
        return {appID, success: true};
      });
    });
