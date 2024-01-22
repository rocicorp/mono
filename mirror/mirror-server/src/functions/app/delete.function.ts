import {FieldValue, Timestamp, type Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {
  deleteAppRequestSchema,
  deleteAppResponseSchema,
} from 'mirror-protocol/src/app.js';
import {
  apiKeyDataConverter,
  apiKeysCollection,
} from 'mirror-schema/src/api-key.js';
import {appDataConverter, appPath} from 'mirror-schema/src/app.js';
import {
  DeploymentSpec,
  deploymentsCollection,
} from 'mirror-schema/src/deployment.js';
import {envsCollection} from 'mirror-schema/src/env.js';
import {
  appNameIndexPath,
  teamDataConverter,
  teamPath,
} from 'mirror-schema/src/team.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {getDataOrFail} from '../validators/data.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';
import {requestDeployment} from './deploy.function.js';

const NULL_SPEC: DeploymentSpec = {
  appModules: [],
  serverVersion: '',
  serverVersionRange: '',
  hostname: '',
  envUpdateTime: Timestamp.fromMillis(0),
};

// Note: 'delete' is a reserved word, so we have to call the variable something else.
export const deleteApp = (firestore: Firestore) =>
  validateSchema(deleteAppRequestSchema, deleteAppResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore, ['admin']))
    .handle(async (request, context) => {
      const {appID} = request;
      const {userID} = context;

      const deploymentPath = await requestDeployment(firestore, appID, {
        requesterID: userID,
        type: 'DELETE',
        spec: NULL_SPEC,
      });
      logger.info(`Requested delete of app ${appID}`);
      return {success: true, deploymentPath};
    });

// Called by the deployment executor when the worker in Cloudflare has been deleted.
export async function deleteAppDocs(
  firestore: Firestore,
  appID: string,
): Promise<void> {
  const appDocRef = firestore
    .doc(appPath(appID))
    .withConverter(appDataConverter);
  await firestore.runTransaction(async txn => {
    const app = getDataOrFail(
      await txn.get(appDocRef),
      'internal',
      `App ${appID} concurrently deleted?`,
    );

    const {teamID, name: appName} = app;

    const [envs, deployments, keys] = await Promise.all([
      txn.get(firestore.collection(envsCollection(appID)).select()),
      txn.get(firestore.collection(deploymentsCollection(appID)).select()),
      txn.get(
        firestore
          .collection(apiKeysCollection(teamID))
          .withConverter(apiKeyDataConverter)
          .where('appIDs', 'array-contains', appID),
      ),
      // Note: We keep the metricsCollection for billing / usage purposes.
    ]);

    // Delete all documents associated with the App.
    //
    // 1. The App doc itself.
    txn.delete(appDocRef);
    // 2. All of its in environments.
    envs.forEach(doc => txn.delete(doc.ref));
    // 3. All of its deployments.
    //    TODO(darick): Clean up orphaned modules in GCS.
    deployments.forEach(doc => txn.delete(doc.ref));
    // 4. The app name index entry for the team.
    txn.delete(firestore.doc(appNameIndexPath(teamID, appName)));
    // 5. Remove the appID from API keys, and delete the API key that only has the appID.
    keys.docs.forEach(doc => {
      const {appIDs} = doc.data();
      if (appIDs.length === 1) {
        // TODO: Don't delete if the key has the forthcoming "apps:create" permission.
        txn.delete(doc.ref);
      } else {
        txn.update(doc.ref, {appIDs: FieldValue.arrayRemove(appID)});
      }
    });
    // 6. Finally, decrement the Team's `numApps` field.
    //    Note that this is a "blind" update that doesn't require
    //    reading/locking the Team doc in the Transaction.
    txn.update(
      firestore.doc(teamPath(teamID)).withConverter(teamDataConverter),
      {numApps: FieldValue.increment(-1)},
    );
  });
}
