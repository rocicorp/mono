import type {Firestore} from 'firebase-admin/firestore';
import {HttpsError} from 'firebase-functions/v2/https';
import * as schema from 'mirror-schema/src/server.js';
import {must} from 'shared/src/must.js';

/**
 * Throws an HttpsError if the server module does not exist.
 */
export async function getServerModuleMetadata(
  firestore: Firestore,
  version: string,
): Promise<schema.Server> {
  const doc = await firestore
    .doc(schema.serverPath(version))
    .withConverter(schema.serverDataConverter)
    .get();
  const {exists} = doc;
  if (!exists) {
    throw new HttpsError('not-found', `Version ${version} does not exist`);
  }
  return must(doc.data());
}
