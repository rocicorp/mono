import {FieldValue, type Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {
  deleteAppRequestSchema,
  deleteAppResponseSchema,
} from 'mirror-protocol/src/app.js';
import {appDataConverter, appPath} from 'mirror-schema/src/app.js';
import {
  DeploymentSpec,
  defaultOptions,
  deploymentsCollection,
} from 'mirror-schema/src/deployment.js';
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
  options: defaultOptions(),
  hashesOfSecrets: {},
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
    const deployments = await txn.get(
      firestore.collection(deploymentsCollection(appID)).select(),
    );

    const {teamID, name: appName} = app;

    // Delete all documents associated with the App.
    //
    // 1. The App doc itself.
    txn.delete(appDocRef);
    // 2. All of its deployments.
    //    TODO(darick): Clean up orphaned modules in GCS.
    deployments.forEach(doc => txn.delete(doc.ref));
    // 3. The app name index entry for the team.
    txn.delete(firestore.doc(appNameIndexPath(teamID, appName)));
    // 4. Finally, decrement the Team's `numApps` field.
    //    Note that this is a "blind" update that doesn't require
    //    reading/locking the Team doc in the Transaction.
    txn.update(
      firestore.doc(teamPath(teamID)).withConverter(teamDataConverter),
      {numApps: FieldValue.increment(-1)},
    );
  });
}
