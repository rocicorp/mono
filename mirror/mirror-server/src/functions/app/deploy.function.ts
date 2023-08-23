import {
  Timestamp,
  type Firestore,
  FieldValue,
  Precondition,
} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {logger} from 'firebase-functions';
import {HttpsError} from 'firebase-functions/v2/https';
import {onDocumentCreated} from 'firebase-functions/v2/firestore';
import {
  deploymentPath,
  deploymentDataConverter,
  Deployment,
  DeploymentStatus,
  DeploymentSecrets,
} from 'mirror-schema/src/deployment.js';
import {newDeploymentID} from 'shared/src/mirror/ids.js';
import {getServerModuleMetadata} from '../../cloudflare/get-server-modules.js';
import {publish} from '../../cloudflare/publish.js';
import {appDataConverter, appPath} from 'mirror-schema/src/app.js';
import {must} from 'shared/src/must.js';
import {
  getAppSecrets,
  DEPLOYMENT_SECRETS_NAMES,
  defineSecretSafely,
} from './secrets.js';
import {watch} from 'mirror-schema/src/watch.js';
import {toMillis} from 'mirror-schema/src/timestamp.js';

// This is the API token for reflect-server.net
// https://dash.cloudflare.com/085f6d8eb08e5b23debfb08b21bda1eb/
const cloudflareApiToken = defineSecretSafely('CLOUDFLARE_API_TOKEN');

export const deploy = (firestore: Firestore, storage: Storage) =>
  onDocumentCreated(
    {
      document: 'apps/{appID}/deployments/{deploymentID}',
      secrets: ['CLOUDFLARE_API_TOKEN', ...DEPLOYMENT_SECRETS_NAMES],
    },
    async event => {
      const {appID, deploymentID} = event.params;

      await earlierDeployments(firestore, appID, deploymentID);
      await runDeployment(firestore, storage, appID, deploymentID);
    },
  );

export type PublishFn = typeof publish;

export async function runDeployment(
  firestore: Firestore,
  storage: Storage,
  appID: string,
  deploymentID: string,
  publishToCloudflare: PublishFn = publish, // Overridden in tests.
): Promise<void> {
  const [appDoc, deploymentDoc] = await firestore.runTransaction(
    tx =>
      Promise.all([
        tx.get(firestore.doc(appPath(appID)).withConverter(appDataConverter)),
        tx.get(
          firestore
            .doc(deploymentPath(appID, deploymentID))
            .withConverter(deploymentDataConverter),
        ),
      ]),
    {readOnly: true},
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

  const {cfID, cfScriptName} = must(appDoc.data());
  const {
    status,
    spec: {serverVersion, hostname, options, appModules},
  } = must(deploymentDoc.data());

  if (status !== 'REQUESTED') {
    logger.warn(`Deployment is already ${status}`);
    return;
  }

  const config = {
    accountID: cfID,
    scriptName: cfScriptName,
    apiToken: cloudflareApiToken.value(),
  } as const;

  const {modules: serverModules} = await getServerModuleMetadata(
    firestore,
    serverVersion,
  );

  const {secrets, hashes} = await getAppSecrets();

  const lastUpdateTime = must(deploymentDoc.updateTime);
  await setDeploymentStatus(
    firestore,
    appID,
    deploymentID,
    'DEPLOYING',
    undefined,
    {lastUpdateTime}, // Aborts if another trigger is already publishing the same deployment.
  );
  try {
    await publishToCloudflare(
      storage,
      config,
      hostname,
      options,
      secrets,
      appModules,
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

  await setRunningDeployment(firestore, appID, deploymentID, hashes);
}

export function requestDeployment(
  firestore: Firestore,
  appID: string,
  data: Pick<Deployment, 'requesterID' | 'type' | 'spec'>,
): Promise<string> {
  const appDocRef = firestore
    .doc(appPath(appID))
    .withConverter(appDataConverter);
  return firestore.runTransaction(async tx => {
    for (let i = 0; i < 5; i++) {
      const deploymentID = newDeploymentID();
      const docRef = firestore
        .doc(deploymentPath(appID, deploymentID))
        .withConverter(deploymentDataConverter);
      const doc = await tx.get(docRef);
      if (doc.exists) {
        logger.warn(`Deployment ${deploymentID} already exists. Trying again.`);
        continue;
      }
      const deployment: Deployment = {
        ...data,
        deploymentID,
        status: 'REQUESTED',
        requestTime: FieldValue.serverTimestamp() as Timestamp,
      };

      tx.create(docRef, deployment);
      tx.update(appDocRef, {
        queuedDeploymentIDs: FieldValue.arrayUnion(deploymentID),
      });
      return docRef.path;
    }
    throw new HttpsError(
      'resource-exhausted',
      'Failed to generate unused deployment ID',
    );
  });
}

const DEPLOYMENT_FAILURE_TIMEOUT_MS = 1000 * 60; // 1 minute.

export async function earlierDeployments(
  firestore: Firestore,
  appID: string,
  deploymentID: string,
  setTimeoutFn = setTimeout,
): Promise<void> {
  const appDoc = firestore.doc(appPath(appID)).withConverter(appDataConverter);
  let failureTimeout: NodeJS.Timer | undefined;

  for await (const appSnapshot of watch(appDoc)) {
    clearTimeout(failureTimeout);

    if (!appSnapshot.exists) {
      throw new HttpsError('not-found', `App ${appID} has been deleted.`);
    }
    const app = must(appSnapshot.data());
    if (!app.queuedDeploymentIDs?.length) {
      throw new HttpsError(
        'aborted',
        `Deployment ${deploymentID} is no longer queued.`,
      );
    }
    const nextDeploymentID = app.queuedDeploymentIDs[0];
    if (nextDeploymentID === deploymentID) {
      return; // Common case: the deployment is next in line.
    }
    logger.info(
      `Waiting for deployments ahead of ${deploymentID}: ${app.queuedDeploymentIDs}`,
    );

    const deploymentDoc = await firestore
      .doc(deploymentPath(appID, nextDeploymentID))
      .withConverter(deploymentDataConverter)
      .get();
    const deployment = must(deploymentDoc.data());
    const lastUpdateTime = must(deploymentDoc.updateTime);
    const lastActionTime = deployment.deployTime ?? deployment.requestTime;
    failureTimeout = setTimeoutFn(async () => {
      await setDeploymentStatus(
        firestore,
        appID,
        nextDeploymentID,
        'FAILED',
        'Deployment timed out',
        {lastUpdateTime},
      );
      logger.warn(`Set ${nextDeploymentID} to FAILED after timeout`);
    }, toMillis(lastActionTime) + DEPLOYMENT_FAILURE_TIMEOUT_MS - Date.now());
  }
}

function deploymentUpdate(
  status: Exclude<DeploymentStatus, 'REQUESTED'>,
  statusMessage?: string,
): Partial<Deployment> {
  const update: Partial<Deployment> = {status};
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
  status: 'DEPLOYING' | 'FAILED',
  statusMessage?: string,
  precondition: Precondition = {},
): Promise<void> {
  const batch = firestore.batch();
  batch.update(
    firestore
      .doc(deploymentPath(appID, deploymentID))
      .withConverter(deploymentDataConverter),
    deploymentUpdate(status, statusMessage),
    precondition,
  );
  if (status !== 'DEPLOYING') {
    batch.update(
      firestore.doc(appPath(appID)).withConverter(appDataConverter),
      {
        queuedDeploymentIDs: FieldValue.arrayRemove(deploymentID),
      },
    );
  }
  await batch.commit();
}

/**
 * Updates the collection of deployments so that the specified one
 * is `RUNNING`, setting the previously `RUNNING` deployment to `STOPPED`.
 */
async function setRunningDeployment(
  firestore: Firestore,
  appID: string,
  newDeploymentID: string,
  hashesOfSecrets: DeploymentSecrets,
): Promise<void> {
  const appDocRef = firestore
    .doc(appPath(appID))
    .withConverter(appDataConverter);
  const newDeploymentDocRef = firestore
    .doc(deploymentPath(appID, newDeploymentID))
    .withConverter(deploymentDataConverter);

  await firestore.runTransaction(async tx => {
    const [appDoc, newDeploymentDoc] = await Promise.all([
      tx.get(appDocRef),
      tx.get(newDeploymentDocRef),
    ]);
    if (!appDoc.exists) {
      throw new HttpsError('internal', `Missing ${appID} App doc`);
    }
    if (!newDeploymentDoc.exists) {
      throw new HttpsError(
        'internal',
        `Missing ${newDeploymentID} Deployment doc`,
      );
    }
    const newDeployment = must(newDeploymentDoc.data());
    newDeployment.spec.hashesOfSecrets = hashesOfSecrets;
    const newRunningDeployment = {
      ...newDeployment,
      ...deploymentUpdate('RUNNING'),
    };

    tx.set(newDeploymentDocRef, newRunningDeployment);
    tx.update(appDocRef, {
      runningDeployment: newRunningDeployment,
      queuedDeploymentIDs: FieldValue.arrayRemove(newDeploymentID),
    });

    const oldDeploymentID = appDoc.data()?.runningDeployment?.deploymentID;
    if (oldDeploymentID) {
      const oldDeploymentDoc = firestore
        .doc(deploymentPath(appID, oldDeploymentID))
        .withConverter(deploymentDataConverter);
      tx.update(oldDeploymentDoc, deploymentUpdate('STOPPED'));
    }
  });
}
