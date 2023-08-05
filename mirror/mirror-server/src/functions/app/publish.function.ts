import type {Firestore} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {logger} from 'firebase-functions';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  publishRequestSchema,
  publishResponseSchema,
} from 'mirror-protocol/src/publish.js';
import * as semver from 'semver';
import {isSupportedSemverRange} from 'shared/src/mirror/is-supported-semver-range.js';
import {storeModule, type Module, ModuleRef} from 'mirror-schema/src/module.js';
import {assertAllModulesHaveUniqueNames} from '../../cloudflare/module-assembler.js';
import {findNewestMatchingVersion} from './find-newest-matching-version.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {requestDeployment} from './deploy.function.js';

export const publish = (
  firestore: Firestore,
  storage: Storage,
  bucketName: string,
) =>
  validateSchema(publishRequestSchema, publishResponseSchema)
    .validate(userAuthorization())
    .validate(appAuthorization(firestore))
    .handle(async (publishRequest, context) => {
      const {serverVersionRange, appID} = publishRequest;
      const {userID, app} = context;

      const appModules: Module[] = [
        {...publishRequest.source, type: 'esm'},
        {...publishRequest.sourcemap, type: 'text'},
      ];
      assertAllModulesHaveUniqueNames(appModules, 'invalid-argument');

      if (semver.validRange(serverVersionRange) === null) {
        throw new HttpsError('invalid-argument', 'Invalid desired version');
      }

      const range = new semver.Range(serverVersionRange);
      if (!isSupportedSemverRange(range)) {
        throw new HttpsError('invalid-argument', 'Unsupported desired version');
      }

      const serverVersion = await findNewestMatchingVersion(firestore, range);
      logger.log(
        `Found matching version for ${serverVersionRange}: ${serverVersion}`,
      );

      const appModuleRefs = await saveToGoogleCloudStorage(
        storage,
        bucketName,
        appModules,
      );

      const deploymentPath = await requestDeployment(firestore, appID, {
        requesterID: userID,
        type: 'USER_UPLOAD',
        appModules: appModuleRefs,
        hostname: `${app.name}.reflect-server.net`,
        // appVersion
        // description
        serverVersionRange,
        serverVersion,
      });

      logger.log(`Requested ${deploymentPath}`);
      return {deploymentPath, success: true};
    });

function saveToGoogleCloudStorage(
  storage: Storage,
  bucketName: string,
  appModules: Module[],
): Promise<ModuleRef[]> {
  const bucket = storage.bucket(bucketName);
  return Promise.all(appModules.map(ref => storeModule(bucket, ref)));
}
