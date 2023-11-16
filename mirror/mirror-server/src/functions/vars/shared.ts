import type {Firestore, Timestamp} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  deploymentDataConverter,
  deploymentsCollection,
} from 'mirror-schema/src/deployment.js';
import {TimeoutError, watch} from 'mirror-schema/src/watch.js';

const DEPLOYMENT_WAIT_TIMEOUT = 10000;

export async function deploymentAtOrAfter(
  firestore: Firestore,
  appID: string,
  updateTime: Timestamp,
): Promise<{success: true; deploymentPath?: string}> {
  logger.info(
    `Waiting for deployment of app ${appID} after ${updateTime
      .toDate()
      .toISOString()}`,
  );
  try {
    for await (const snapshot of watch(
      firestore
        .collection(deploymentsCollection(appID))
        .withConverter(deploymentDataConverter)
        .where('requestTime', '>=', updateTime)
        .orderBy('requestTime')
        .limitToLast(1),
      DEPLOYMENT_WAIT_TIMEOUT,
    )) {
      if (snapshot.size >= 1) {
        const deploymentPath = snapshot.docs[0].ref.path;
        logger.info(`Deployment: ${deploymentPath}`);
        return {success: true, deploymentPath};
      }
    }
  } catch (e) {
    if (e instanceof TimeoutError) {
      logger.warn(`Timed out waiting for redeployment of ${appID}`);
      throw new HttpsError(
        'deadline-exceeded',
        'Timed out deploying changes to Server Variables. Please try again later.',
      );
    } else {
      throw e;
    }
  }
  return {success: true};
}
