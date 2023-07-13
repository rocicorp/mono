import {Timestamp, type Firestore} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {defineSecret, defineString} from 'firebase-functions/params';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  publishRequestSchema,
  publishResponseSchema,
} from 'mirror-protocol/src/publish.js';
import * as schema from 'mirror-schema/src/deployment.js';
import * as semver from 'semver';
import {newDeploymentID} from 'shared/src/mirror/ids.js';
import {isSupportedSemverRange} from 'shared/src/mirror/is-supported-semver-range.js';
import type {CfModule} from '../cloudflare/create-worker-upload-form.js';
import {getServerModuleMetadata} from '../cloudflare/get-server-modules.js';
import {publish as publishToCloudflare} from '../cloudflare/publish.js';
import {findNewestMatchingVersion} from '../find-newest-matching-version.js';
import {storeModule} from '../store-module.js';
import {withAuthorization} from './validators/auth.js';
import {withSchema} from './validators/schema.js';

// This is the API token for reflect-server.net
// https://dash.cloudflare.com/085f6d8eb08e5b23debfb08b21bda1eb/
const cloudflareApiToken = defineSecret('CLOUDFLARE_API_TOKEN');
const cloudflareAccountId = defineString('CLOUDFLARE_ACCOUNT_ID');

export const publish = (
  firestore: Firestore,
  storage: Storage,
  bucketName: string,
) =>
  withSchema(
    publishRequestSchema,
    publishResponseSchema,
    withAuthorization(async (publishRequest, context) => {
      const {serverVersionRange, name: appName, appID} = publishRequest;
      const userID = context.auth.uid;

      if (!userCanPublishApp(userID, appID)) {
        throw new HttpsError(
          'permission-denied',
          'User does not have permission to publish this app',
        );
      }

      if (semver.validRange(serverVersionRange) === null) {
        throw new HttpsError('invalid-argument', 'Invalid desired version');
      }

      const range = new semver.Range(serverVersionRange);
      if (!isSupportedSemverRange(range)) {
        throw new HttpsError('invalid-argument', 'Unsupported desired version');
      }

      const version = await findNewestMatchingVersion(firestore, range);
      console.log(
        `Found matching version for ${serverVersionRange}: ${version}`,
      );

      const config = {
        accountID: cloudflareAccountId.value(),
        // TODO(arv): This is not the right name.
        scriptName: appName,
        apiToken: cloudflareApiToken.value(),
      } as const;
      const appModule: CfModule = {
        ...publishRequest.source,
        type: 'esm',
      };
      const appSourcemapModule: CfModule = {
        ...publishRequest.sourcemap,
        type: 'text',
      };

      const [appModuleURL, appSourcemapURL] = await saveToGoogleCloudStorage(
        storage,
        bucketName,
        appModule,
        appSourcemapModule,
      );

      const serverModuleMetadata = await getServerModuleMetadata(
        firestore,
        version,
      );

      const deploymentID = await saveToFirestore(firestore, appID, {
        requesterID: userID,
        type: 'USER_UPLOAD',
        appModule: appModuleURL,
        appSourcemap: appSourcemapURL,
        // appVersion
        // description
        serverVersionRange,
        serverModules: serverModuleMetadata.modules.map(m => m.url),
        status: 'DEPLOYING',
        statusTime: Timestamp.now(),
      });

      console.log(`Saved deployment ${deploymentID} to firestore`);

      try {
        await publishToCloudflare(
          firestore,
          storage,
          config,
          appModule,
          appSourcemapModule,
          appName,
          version,
        );
      } catch (e) {
        await setDeploymentStatus(firestore, appID, deploymentID, 'FAILED');
        throw e;
      }

      await setDeploymentStatusOfAll(firestore, appID, deploymentID);

      return {success: true};
    }),
  );

function userCanPublishApp(userID: string, appID: string): boolean {
  console.warn(
    `userCanPublishApp(${userID}, ${appID}) not implemented. Allowing publish for now`,
  );

  return true;
}
/**
 * Returns the URL (gs://...) of the uploaded file.
 */
function saveToGoogleCloudStorage(
  storage: Storage,
  bucketName: string,
  appModule: CfModule,
  appSourcemapModule: CfModule,
): Promise<[appModuleURL: string, appSourcemapModuleURL: string]> {
  const bucket = storage.bucket(bucketName);
  return Promise.all([
    storeModule(bucket, appModule),
    storeModule(bucket, appSourcemapModule),
  ]);
}

async function saveToFirestore(
  firestore: Firestore,
  appID: string,
  data: schema.Deployment,
): Promise<string> {
  const deploymentID = newDeploymentID();

  const docRef = firestore
    .doc(schema.deploymentPath(appID, deploymentID))
    .withConverter(schema.deploymentDataConverter);

  await firestore.runTransaction(async tx => {
    const doc = await tx.get(docRef);
    if (doc.exists) {
      throw new HttpsError(
        'already-exists',
        'A deployment with this ID already exists',
      );
    }

    tx.create(docRef, data);
  });

  return deploymentID;
}

async function setDeploymentStatus(
  firestore: Firestore,
  appID: string,
  deploymentID: string,
  status: schema.DeploymentStatus,
): Promise<void> {
  const docRef = firestore
    .doc(schema.deploymentPath(appID, deploymentID))
    .withConverter(schema.deploymentDataConverter);

  await firestore.runTransaction(async tx => {
    const doc = await tx.get(docRef);
    if (!doc.exists) {
      throw new HttpsError(
        'not-found',
        'A deployment with this ID does not exist',
      );
    }

    tx.update(docRef, {status, statusTime: Timestamp.now()});
  });
}

/**
 * Updates the the status and statusTime of all deployments for the app.
 */
async function setDeploymentStatusOfAll(
  firestore: Firestore,
  appID: string,
  deploymentID: string,
): Promise<void> {
  const ref = firestore
    .collection(schema.deploymentsCollection(appID))
    .withConverter(schema.deploymentDataConverter);
  const refs = await ref.listDocuments();
  await firestore.runTransaction(async tx => {
    const docs = await tx.getAll(...refs);

    for (const doc of docs) {
      if (!doc.exists) {
        continue;
      }

      tx.update(doc.ref, {
        status: doc.id === deploymentID ? 'RUNNING' : 'STOPPED',
        statusTime: Timestamp.now(),
      });
    }
  });
}
