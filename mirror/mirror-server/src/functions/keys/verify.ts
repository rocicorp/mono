import type {Firestore, QueryDocumentSnapshot} from 'firebase-admin/firestore';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  APP_KEY_COLLECTION_ID,
  appKeyDataConverter,
  type AppKey,
} from 'mirror-schema/src/api-key.js';

export async function verifyKey(
  firestore: Firestore,
  key: string,
): Promise<QueryDocumentSnapshot<AppKey>> {
  const query = await firestore
    .collectionGroup(APP_KEY_COLLECTION_ID)
    .withConverter(appKeyDataConverter)
    .where('value', '==', key)
    .get();
  if (query.size === 0) {
    throw new HttpsError('permission-denied', `Invalid key`);
  }
  if (query.size > 1) {
    throw new HttpsError(
      'failed-precondition',
      `Multiple keys matched.`, // Should never happen (unless random is broken), but fail to be safe.
    );
  }
  return query.docs[0];
}
