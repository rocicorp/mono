import {Timestamp, getFirestore} from 'firebase-admin/firestore';
import {deploymentsCollection} from 'mirror-schema/src/deployment.js';

export async function runQueryHandler() {
  console.info('Running query ...');
  const query = await getFirestore()
    .collection(deploymentsCollection('fooAppID'))
    .where('requesterID', '==', 'mirror-server')
    .where('requestTime', '>=', Timestamp.fromMillis(Date.now() - 1000 * 60))
    .count()
    .get();
  console.log('Result: ', query.data().count);
}
