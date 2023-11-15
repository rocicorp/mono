import {getFirestore} from 'firebase-admin/firestore';

export async function runQueryHandler() {
  console.info('Running query ...');
  const query = await getFirestore().collectionGroup('metrics').get();
  console.log(
    'Result: ',
    query.docs.map(doc => doc.ref.path),
  );
}
