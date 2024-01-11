import {getFirestore} from 'firebase-admin/firestore';

export async function runQueryHandler() {
  console.info('Running query ...');
  const query = await getFirestore()
    .collectionGroup('metrics')
    .where('yearMonth', '==', 202311)
    .where('appID', '!=', null)
    .get();
  console.log(
    'Result: ',
    query.docs.map(doc => doc.ref.path),
  );
}
