import type {Firestore} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {logger} from 'firebase-functions';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  publishRequestSchema,
  publishResponseSchema,
} from 'mirror-protocol/src/publish.js';
import {DistTag} from 'mirror-protocol/src/version.js';
import type {App} from 'mirror-schema/src/app.js';
import type {DeploymentSpec} from 'mirror-schema/src/deployment.js';
import {ModuleRef, storeModule, type Module} from 'mirror-schema/src/module.js';
import {
  providerDataConverter,
  providerPath,
} from 'mirror-schema/src/provider.js';
import * as semver from 'semver';
import {gtr} from 'semver';
import {isSupportedSemverRange} from 'shared/src/mirror/is-supported-semver-range.js';
import {assertAllModulesHaveUniqueNames} from '../../cloudflare/module-assembler.js';
import {SecretsCache, type Secrets} from '../../secrets/index.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {getDataOrFail} from '../validators/data.js';
import {validateSchema} from '../validators/schema.js';
import {DistTags, userAgentVersion} from '../validators/version.js';
import {requestDeployment} from './deploy.function.js';
import {findNewestMatchingVersion} from './find-newest-matching-version.js';
import {getAppSecrets} from './secrets.js';

export const publish = (
  firestore: Firestore,
  secretsClient: Secrets,
  storage: Storage,
  bucketName: string,
  testDistTags?: DistTags,
) =>
  validateSchema(publishRequestSchema, publishResponseSchema)
    .validate(userAgentVersion(testDistTags))
    .validate(userAuthorization())
    .validate(appAuthorization(firestore))
    .handle(async (publishRequest, context) => {
      const secrets = new SecretsCache(secretsClient);
      const {
        serverVersionRange,
        appID,
        serverReleaseChannel: newServerReleaseChannel,
      } = publishRequest;
      const {userID, app, distTags} = context;

      const serverReleaseChannel =
        newServerReleaseChannel ?? app.serverReleaseChannel;

      const appModules: Module[] = [
        {...publishRequest.source, type: 'esm'},
        {...publishRequest.sourcemap, type: 'text'},
      ];
      assertAllModulesHaveUniqueNames(appModules, 'invalid-argument');

      const spec = await computeDeploymentSpec(
        firestore,
        secrets,
        {
          ...app,
          serverReleaseChannel,
        },
        serverVersionRange,
      );

      if (app.runningDeployment === undefined) {
        // If this is the first publish to the App, disallow deprecated server version ranges.
        const minNonDeprecated = distTags[DistTag.MinNonDeprecated];
        if (minNonDeprecated && gtr(minNonDeprecated, serverVersionRange)) {
          throw new HttpsError(
            'out-of-range',
            `The app depends on a deprecated version of Reflect (${serverVersionRange}). Please update to @rocicorp/reflect/latest and try again.`,
          );
        }
      }

      const appModuleRefs = await saveToGoogleCloudStorage(
        storage,
        bucketName,
        appModules,
      );

      const deploymentPath = await requestDeployment(
        firestore,
        appID,
        {
          requesterID: userID,
          type: 'USER_UPLOAD',
          spec: {
            ...spec,
            appModules: appModuleRefs,
            // appVersion
            // description
          },
        },
        serverReleaseChannel,
      );

      logger.log(`Requested ${deploymentPath}`);
      return {deploymentPath, success: true};
    });

export async function computeDeploymentSpec(
  firestore: Firestore,
  secrets: Secrets,
  app: App,
  serverVersionRange: string,
): Promise<Omit<DeploymentSpec, 'appModules' | 'appVersion' | 'description'>> {
  if (semver.validRange(serverVersionRange) === null) {
    throw new HttpsError('invalid-argument', 'Invalid desired version');
  }

  const range = new semver.Range(serverVersionRange);
  if (!isSupportedSemverRange(range)) {
    throw new HttpsError('invalid-argument', 'Unsupported desired version');
  }

  const {serverReleaseChannel, secrets: appSecrets = {}} = app;
  const serverVersion = await findNewestMatchingVersion(
    firestore,
    range,
    serverReleaseChannel,
  );
  logger.log(
    `Found matching version for ${serverVersionRange}: ${serverVersion}`,
  );

  const {provider: providerID, name: appName, teamLabel} = app;
  const provider = getDataOrFail(
    await firestore
      .doc(providerPath(providerID))
      .withConverter(providerDataConverter)
      .get(),
    'internal',
    `Provider ${providerID} is not properly set up.`,
  );
  const {
    defaultZone: {zoneName},
  } = provider;

  const {hashes: hashesOfSecrets} = await getAppSecrets(secrets, appSecrets);

  return {
    serverVersionRange,
    serverVersion,
    // Note: Hyphens are not allowed in teamLabels.
    hostname: `${appName}-${teamLabel}.${zoneName}`,
    options: app.deploymentOptions,
    hashesOfSecrets,
  };
}

function saveToGoogleCloudStorage(
  storage: Storage,
  bucketName: string,
  appModules: Module[],
): Promise<ModuleRef[]> {
  const bucket = storage.bucket(bucketName);
  return Promise.all(appModules.map(ref => storeModule(bucket, ref)));
}
