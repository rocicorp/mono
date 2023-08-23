import type {Firestore} from 'firebase-admin/firestore';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  SERVER_COLLECTION,
  serverDataConverter,
} from 'mirror-schema/src/server.js';
import type {Range} from 'semver';

export async function findNewestMatchingVersion(
  firestore: Firestore,
  serverVersionRange: Range,
  serverReleaseChannel: string,
): Promise<string> {
  const versions = await firestore
    .collection(SERVER_COLLECTION)
    .withConverter(serverDataConverter)
    .where('channels', 'array-contains', serverReleaseChannel)
    .orderBy('major', 'desc')
    .orderBy('minor', 'desc')
    .orderBy('patch', 'desc')
    .select()
    .get();

  for (const doc of versions.docs) {
    const version = doc.id;
    if (serverVersionRange.test(version)) {
      return version;
    }
  }

  // TODO(darick): For non standard release channels (e.g. "debug-then-forgot"),
  // consider logging a warning here and then falling back to the "stable" release.

  throw new HttpsError(
    'out-of-range',
    `No matching version for ${serverVersionRange} found`,
  );
}
