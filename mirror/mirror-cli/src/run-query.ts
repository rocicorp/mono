import {Timestamp, getFirestore} from 'firebase-admin/firestore';
import {
  APP_DEPLOYMENTS_COLLECTION_ID,
  deploymentDataConverter,
} from 'mirror-schema/src/deployment.js';

export async function runQueryHandler() {
  console.info('Running query ...');
  const query = await getFirestore()
    .collectionGroup(APP_DEPLOYMENTS_COLLECTION_ID)
    .withConverter(deploymentDataConverter)
    .where('type', '==', 'SERVER_UPDATE')
    .where('requestTime', '>', Timestamp.fromMillis(Date.now() - 1_000_000))
    .select('status')
    .get();
  console.log('Result: ', query.size);
}
