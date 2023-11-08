import type {Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {onDocumentUpdated} from 'firebase-functions/v2/firestore';
import {appDataConverter} from 'mirror-schema/src/app.js';
import {appPath} from 'mirror-schema/src/deployment.js';
import {envDataConverter, envPath} from 'mirror-schema/src/env.js';
import {must} from 'shared/src/must.js';

// Propagates the Env document's updateTime to the App document's
// `envUpdateTime` field to kick off a new deployment.
//
// An alternative would have been to transactionally update the App doc's
// `envUpdateTime` field on writes to the Env doc, but the current approach
// has a couple of advantages:
// (1) It handles manual editing of DeploymentOptions via the Firebase console.
// (2) It can scale to more than 499 "instances" / deployments (a Firestore write
//     can modify a maximum of 500 documents).
export const autoDeploy = (firestore: Firestore) =>
  onDocumentUpdated(
    {
      document: 'apps/{appID}/envs/{envName}',
      // The logic in this function is pure Firestore with no interaction
      // with external systems, so retries should be a safe thing to enable.
      retry: true,
    },
    event => {
      const {appID, envName} = event.params;
      const appDocRef = firestore
        .doc(appPath(appID))
        .withConverter(appDataConverter);
      const envDocRef = firestore
        .doc(envPath(appID, envName))
        .withConverter(envDataConverter);

      return firestore.runTransaction(async tx => {
        const [appDoc, envDoc] = await Promise.all([
          tx.get(appDocRef),
          tx.get(envDocRef),
        ]);

        if (!appDoc.exists) {
          logger.warn(`App ${appID} has been deleted.`);
          return;
        }
        if (!envDoc.exists) {
          logger.warn(`Env ${envName} for App ${appID} has been deleted.`);
          return;
        }
        const app = must(appDoc.data());
        const envUpdateTime = must(envDoc.updateTime);
        if (app.envUpdateTime.toMillis() >= envUpdateTime.toMillis()) {
          // Technically possible out-of-order execution.
          logger.info(`App ${appID} envUpdateTime is already up to date`);
          return;
        }
        logger.info(
          `Updating App ${appID} envUpdateTime to ${envUpdateTime
            .toDate()
            .toISOString()}`,
        );
        tx.update(appDocRef, {envUpdateTime});
      });
    },
  );
