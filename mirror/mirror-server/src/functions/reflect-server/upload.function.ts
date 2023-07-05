import type {Bucket} from '@google-cloud/storage';
import type {Firestore} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {HttpsError} from 'firebase-functions/v2/https';
import * as protocol from 'mirror-protocol/src/reflect-server.js';
import * as schema from 'mirror-schema/src/reflect-server.js';
import {nanoid} from 'nanoid';
import {withAdminAuthorization} from '../validators/admin-auth.js';
import {withSchema} from '../validators/schema.js';
import type {AsyncCallable} from '../validators/types.js';

export function upload(
  firestore: Firestore,
  storage: Storage,
  bucketName: string,
): AsyncCallable<protocol.UploadRequest, protocol.UploadResponse> {
  return withSchema(
    protocol.uploadRequestSchema,
    protocol.uploadResponseSchema,
    withAdminAuthorization(async uploadRequest => {
      const bucket = storage.bucket(bucketName);

      const {force, version} = uploadRequest;
      const modules = [uploadRequest.main, ...uploadRequest.modules];
      const filenames = await Promise.all(
        modules.map(m => storeModule(bucket, m)),
      );

      const docRef = firestore
        .doc(schema.reflectServerPath(version))
        .withConverter(schema.reflectServerDataConverter);

      await firestore.runTransaction(async txn => {
        const doc = await txn.get(docRef);
        if (doc.exists && !force) {
          throw new HttpsError(
            'already-exists',
            `Version ${version} already exists`,
          );
        }

        const newDoc: schema.ReflectServerModule = {
          main: {
            name: uploadRequest.main.name,
            filename: filenames[0],
            type: uploadRequest.main.type,
          },
          modules: uploadRequest.modules.map((m, i) => ({
            name: m.name,
            filename: filenames[i + 1],
            type: m.type,
          })),
        };

        txn.set(docRef, newDoc);
      });
      return {success: true};
    }),
  );
}

async function storeModule(bucket: Bucket, module: protocol.Module) {
  const filename = `${encodeURIComponent(module.name)}-${nanoid()}`;
  await bucket.file(filename).save(module.content);
  return filename;
}
