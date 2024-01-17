import type {Firestore} from 'firebase-admin/firestore';
import {
  deleteAppKeysRequestSchema,
  deleteAppKeysResponseSchema,
} from 'mirror-protocol/src/app-keys.js';
import {appKeyPath} from 'mirror-schema/src/api-key.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';

export const deleteFn = (firestore: Firestore) =>
  validateSchema(deleteAppKeysRequestSchema, deleteAppKeysResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore, ['admin']))
    .handle(async request => {
      const {appID, names} = request;

      const deleted = await firestore.runTransaction(async tx => {
        const docs = await tx.getAll(
          ...names.map(name => firestore.doc(appKeyPath(appID, name))),
        );
        const exists: string[] = [];
        docs.forEach(doc => {
          if (doc.exists) {
            exists.push(doc.id);
            tx.delete(doc.ref);
          }
        });
        return exists;
      });
      return {success: true, deleted};
    });
