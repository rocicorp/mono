import type {Firestore} from 'firebase-admin/firestore';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  SERVER_COLLECTION,
  serverDataConverter,
} from 'mirror-schema/src/server.js';
import * as semver from 'semver';

export async function findNewestMatchingVersion(
  firestore: Firestore,
  serverVersionRange: semver.Range,
): Promise<string> {
  const ref = firestore
    .collection(SERVER_COLLECTION)
    .withConverter(serverDataConverter);
  let maxVersion: string | undefined;
  for (const docRef of await ref.listDocuments()) {
    const currentVersion = docRef.id;
    if (serverVersionRange.test(currentVersion)) {
      if (maxVersion === undefined) {
        maxVersion = currentVersion;
      } else if (semver.gt(currentVersion, maxVersion)) {
        maxVersion = currentVersion;
      }
    }
  }

  if (maxVersion === undefined) {
    throw new HttpsError(
      'invalid-argument',
      `No matching version for ${serverVersionRange} found`,
    );
  }

  return maxVersion;
}
