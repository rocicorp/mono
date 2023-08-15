import {Timestamp, type Firestore, FieldValue} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {logger} from 'firebase-functions';
import {defineSecret} from 'firebase-functions/params';
import {HttpsError} from 'firebase-functions/v2/https';
import {onDocumentCreated} from 'firebase-functions/v2/firestore';
import * as schema from 'mirror-schema/src/deployment.js';
import {newDeploymentID} from 'shared/src/mirror/ids.js';
import {getServerModuleMetadata} from '../../cloudflare/get-server-modules.js';
import {publish as publishToCloudflare} from '../../cloudflare/publish.js';
import {App, appDataConverter, appPath} from 'mirror-schema/src/app.js';
import {lockDataConverter, deploymentLockPath} from 'mirror-schema/src/lock.js';
import {Lock} from './lock.js';
import {must} from 'shared/src/must.js';

// This is the API token for reflect-server.net
// https://dash.cloudflare.com/085f6d8eb08e5b23debfb08b21bda1eb/
const cloudflareApiToken = defineSecret('CLOUDFLARE_API_TOKEN');

export const deploy = (firestore: Firestore, storage: Storage) =>
  onDocumentCreated(
    {
      document: 'apps/{appID}/deployments/{deploymentID}',
      secrets: ['CLOUDFLARE_API_TOKEN'],
    },
    async event => {
      const {appID, deploymentID} = event.params;

      const lockDoc = firestore
        .doc(deploymentLockPath(appID))
        .withConverter(lockDataConverter);
      const deploymentLock = new Lock(lockDoc);

      await deploymentLock.withLock(deploymentID, () =>
        deployInLock(firestore, storage, appID, deploymentID),
      );
    },
  );

async function deployInLock(
  firestore: Firestore,
  storage: Storage,
  appID: string,
  deploymentID: string,
): Promise<void> {
  const [appDoc, deploymentDoc] = await firestore.runTransaction(tx =>
    Promise.all([
      tx.get(firestore.doc(appPath(appID)).withConverter(appDataConverter)),
      tx.get(
        firestore
          .doc(schema.deploymentPath(appID, deploymentID))
          .withConverter(schema.deploymentDataConverter),
      ),
    ]),
  );
  if (!appDoc.exists) {
    throw new HttpsError('not-found', `Missing app doc for ${appID}`);
  }
  if (!deploymentDoc.exists) {
    throw new HttpsError(
      'not-found',
      `Missing deployment doc ${deploymentID} for app ${appID}`,
    );
  }

  const app: App = must(appDoc.data());
  const deployment: schema.Deployment = must(deploymentDoc.data());

  if (deployment.status !== 'REQUESTED') {
    logger.warn(`Deployment is already ${deployment.status}`);
    return;
  }

  const config = {
    accountID: app.cfID,
    scriptName: app.cfScriptName,
    apiToken: cloudflareApiToken.value(),
  } as const;

  const {modules: serverModules} = await getServerModuleMetadata(
    firestore,
    deployment.serverVersion,
  );

  await setDeploymentStatus(firestore, appID, deploymentID, 'DEPLOYING');
  try {
    await publishToCloudflare(
      storage,
      config,
      deployment.hostname,
      deployment.appModules,
      serverModules,
    );
  } catch (e) {
    await setDeploymentStatus(
      firestore,
      appID,
      deploymentID,
      'FAILED',
      String(e),
    );
    throw e;
  }

  await setRunningDeployment(firestore, appID, deploymentID);
}

export function requestDeployment(
  firestore: Firestore,
  appID: string,
  data: Pick<
    schema.Deployment,
    | 'requesterID'
    | 'type'
    | 'appModules'
    | 'hostname'
    | 'appVersion'
    | 'description'
    | 'serverVersionRange'
    | 'serverVersion'
  >,
): Promise<string> {
  return firestore.runTransaction(async tx => {
    for (let i = 0; i < 5; i++) {
      const deploymentID = newDeploymentID();
      const docRef = firestore
        .doc(schema.deploymentPath(appID, deploymentID))
        .withConverter(schema.deploymentDataConverter);
      const doc = await tx.get(docRef);
      if (doc.exists) {
        logger.warn(`Deployment ${deploymentID} already exists. Trying again.`);
        continue;
      }
      const deployment: schema.Deployment = {
        ...data,
        status: 'REQUESTED',
        requestTime: FieldValue.serverTimestamp() as Timestamp,
      };

      tx.create(docRef, deployment);
      return docRef.path;
    }
    throw new HttpsError(
      'resource-exhausted',
      'Failed to generate unused deployment ID',
    );
  });
}

function deploymentUpdate(
  status: Exclude<schema.DeploymentStatus, 'REQUESTED'>,
  statusMessage?: string,
): Partial<schema.Deployment> {
  const update: Partial<schema.Deployment> = {status};
  if (statusMessage) {
    update.statusMessage = statusMessage;
  }
  switch (status) {
    case 'DEPLOYING':
      update.deployTime = Timestamp.now();
      break;
    case 'RUNNING':
      update.startTime = Timestamp.now();
      break;
    case 'STOPPED':
    case 'FAILED':
      update.stopTime = Timestamp.now();
      break;
  }
  return update;
}

async function setDeploymentStatus(
  firestore: Firestore,
  appID: string,
  deploymentID: string,
  status: Exclude<schema.DeploymentStatus, 'REQUESTED'>,
  statusMessage?: string,
): Promise<void> {
  await firestore
    .doc(schema.deploymentPath(appID, deploymentID))
    .withConverter(schema.deploymentDataConverter)
    .update(deploymentUpdate(status, statusMessage));
}

/**
 * Updates the collection of deployments so that the specified one
 * is `RUNNING`, setting the previously `RUNNING` deployment to `STOPPED`.
 */
async function setRunningDeployment(
  firestore: Firestore,
  appID: string,
  deploymentID: string,
): Promise<void> {
  const collection = firestore
    .collection(schema.deploymentsCollection(appID))
    .withConverter(schema.deploymentDataConverter);
  await firestore.runTransaction(async tx => {
    const deployments = await tx.get(collection);
    for (const deployment of deployments.docs) {
      if (deployment.id === deploymentID) {
        tx.update(deployment.ref, deploymentUpdate('RUNNING'));
      } else if (deployment.data().status === 'RUNNING') {
        tx.update(deployment.ref, deploymentUpdate('STOPPED'));
      }
    }
  });
}
