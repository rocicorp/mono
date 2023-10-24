import type {Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  createRequestSchema,
  createResponseSchema,
} from 'mirror-protocol/src/app.js';
import type {UserAgent} from 'mirror-protocol/src/user-agent.js';
import {DistTag} from 'mirror-protocol/src/version.js';
import {
  App,
  appDataConverter,
  appPath,
  isValidAppName,
} from 'mirror-schema/src/app.js';
import {defaultOptions} from 'mirror-schema/src/deployment.js';
import {
  providerDataConverter,
  providerPath,
} from 'mirror-schema/src/provider.js';
import {
  appNameIndexDataConverter,
  appNameIndexPath,
  teamDataConverter,
  teamPath,
} from 'mirror-schema/src/team.js';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import {SemVer, coerce, gt, gte} from 'semver';
import {newAppID, newAppIDAsNumber, newAppScriptName} from '../../ids.js';
import {userAuthorization} from '../validators/auth.js';
import {getDataOrFail} from '../validators/data.js';
import {validateSchema} from '../validators/schema.js';
import {DistTags, userAgentVersion} from '../validators/version.js';

export const create = (firestore: Firestore, testDistTags?: DistTags) =>
  validateSchema(createRequestSchema, createResponseSchema)
    .validate(userAgentVersion(testDistTags))
    .validate(userAuthorization())
    .handle((request, context) => {
      const {userID, distTags} = context;
      const {
        requester: {userAgent},
        teamID,
        serverReleaseChannel,
        name: appName,
      } = request;

      if (!teamID || !appName) {
        throw new HttpsError(
          'invalid-argument',
          'Please update to the latest release of @rocicorp/reflect',
        );
      }

      const minNonDeprecated = distTags[DistTag.MinNonDeprecated];
      if (
        minNonDeprecated &&
        gt(minNonDeprecated, new SemVer(userAgent.version))
      ) {
        throw new HttpsError(
          'out-of-range',
          'This version of Reflect is deprecated. Please update to @rocicorp/reflect@latest to create a new app.',
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
        const {defaultMaxApps, dispatchNamespace} = getDataOrFail(
          await txn.get(
            firestore
              .doc(providerPath(defaultProvider))
              .withConverter(providerDataConverter),
          ),
          'internal',
          `Provider ${defaultProvider} is not properly set up.`,
        );
        if (team.numApps >= (team.maxApps ?? defaultMaxApps)) {
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

        if (supportsWorkersForPlatforms(userAgent)) {
          app.scriptRef = {
            namespace: dispatchNamespace,
            name: scriptName,
          };
        }
        txn.update(teamDocRef, {numApps: team.numApps + 1});
        txn.create(appDocRef, app);
        txn.create(appNameDocRef, {appID});
        return {appID, success: true};
      });
    });

export const MIN_WFP_VERSION = new SemVer('0.36.0');

function supportsWorkersForPlatforms(userAgent: UserAgent): boolean {
  const {type: agent, version} = userAgent;
  if (agent !== 'reflect-cli') {
    throw new HttpsError(
      'invalid-argument',
      'Please use @rocicorp/reflect to create and publish apps.',
    );
  }
  // coerce to treat pre-releases equally.
  if (gte(coerce(version) ?? version, MIN_WFP_VERSION)) {
    logger.info(`Creating WFP app for reflect-cli v${version}`);
    return true;
  }
  logger.info(`Creating legacy app for reflect-cli v${version}`);
  return false;
}
