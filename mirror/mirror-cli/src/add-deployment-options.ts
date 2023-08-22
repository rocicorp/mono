import {getFirestore} from 'firebase-admin/firestore';
import {APP_COLLECTION} from 'mirror-schema/src/app.js';
import {defaultOptions} from 'mirror-schema/src/deployment.js';

export async function addDeploymentsOptionsHandler() {
  const firestore = getFirestore();
  const deployments = await firestore.collectionGroup(APP_COLLECTION).get();
  let batch = firestore.batch();
  let size = 0;
  for (const doc of deployments.docs) {
    if (doc.data().deploymentOptions === undefined) {
      batch.update(doc.ref, {deploymentOptions: defaultOptions()});
      size++;
    }
    if (size === 500) {
      console.info(`Setting deploymentOptions on ${size} Apps`);
      await batch.commit();
      batch = firestore.batch();
      size = 0;
    }
  }
  console.info(`Setting deploymentOptions on ${size} Apps`);
  await batch.commit();
}
