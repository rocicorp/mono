import type {Firestore} from 'firebase-admin/firestore';
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
  providerDataConverter,
  providerPath,
} from 'mirror-schema/src/provider.js';
import {defaultOptions} from 'mirror-schema/src/deployment.js';
import {
  appNameIndexDataConverter,
  appNameIndexPath,
  teamDataConverter,
  teamPath,
} from 'mirror-schema/src/team.js';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import {newAppID, newAppIDAsNumber, newAppScriptName} from '../../ids.js';
import {userAuthorization} from '../validators/auth.js';
import {getDataOrFail} from '../validators/data.js';
import {validateSchema} from '../validators/schema.js';

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
        const user = getDataOrFail(
          await txn.get(userDocRef),
          'not-found',
          `User ${userID} does not exist`,
        );

        const role = user.roles[teamID];
        if (role !== 'admin') {
          throw new HttpsError(
            'permission-denied',
            `User ${userID} does not have permission to create new apps for team ${teamID}`,
          );
        }

        // Check app limits
        const team = getDataOrFail(
          await txn.get(teamDocRef),
          'not-found',
          `Team ${teamID} does not exist`,
        );
        // TODO: To support onprem, allow apps to be created for a specific provider with
        //       appropriate authorization.
        const {defaultProvider} = team;
        const provider = getDataOrFail(
          await txn.get(
            firestore
              .doc(providerPath(defaultProvider))
              .withConverter(providerDataConverter),
          ),
          'internal',
          `Provider ${defaultProvider} is not properly set up.`,
        );
        if (team.numApps >= (team.maxApps ?? provider.defaultMaxApps)) {
          throw new HttpsError(
            'resource-exhausted',
            `Maximum number of apps reached. Use 'npx @rocicorp/reflect delete' to clean up old apps.`,
          );
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
          teamLabel: team.label,
          teamSubdomain: '', // Deprecated
          provider: defaultProvider,
          cfID: 'deprecated',
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
