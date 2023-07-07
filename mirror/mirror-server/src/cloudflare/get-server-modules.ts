import type {Firestore} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {HttpsError} from 'firebase-functions/v2/https';
import * as schema from 'mirror-schema/src/server.js';
import assert from 'node:assert';
import {parseCloudStorageURL} from '../parse-cloud-storage-url.js';
import type {CfModule} from './create-worker-upload-form.js';

export async function getServerModules(
  firestore: Firestore,
  storage: Storage,
  version: string,
): Promise<CfModule[]> {
  const server = await getServerModuleMetadata(firestore, version);
  const {modules} = server;

  const allModules = await Promise.all(
    modules.map(async module => {
      const {name, url, type} = module;
      const {bucketName, filename} = parseCloudStorageURL(url);
      const bucket = storage.bucket(bucketName);
      const content = await bucket.file(filename).download();
      return {name, content: content[0].toString('utf-8'), type};
    }),
  );

  return allModules;
}

/**
 * Throws an HttpsError if the server module does not exist.
 */
export async function getServerModuleMetadata(
  firestore: Firestore,
  version: string,
): Promise<schema.Server> {
  const docRef = firestore
    .doc(schema.serverPath(version))
    .withConverter(schema.serverDataConverter);

  const serverModule = await firestore.runTransaction(
    async txn => {
      const doc = await txn.get(docRef);
      const {exists} = doc;
      if (!exists) {
        throw new HttpsError('not-found', `Version ${version} does not exist`);
      }

      return doc.data();
    },
    {readOnly: true},
  );
  assert(serverModule);

  return serverModule;
}
