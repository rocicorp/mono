import type {Firestore} from 'firebase-admin/firestore';
import {
  deleteAppKeysRequestSchema,
  deleteAppKeysResponseSchema,
} from 'mirror-protocol/src/app-keys.js';
import {apiKeyPath} from 'mirror-schema/src/api-key.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';

export const deleteFn = (firestore: Firestore) =>
  validateSchema(deleteAppKeysRequestSchema, deleteAppKeysResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore, ['admin']))
    .handle(async (request, context) => {
      const {names} = request;
      const {
        app: {teamID},
      } = context;

      const deleted = await firestore.runTransaction(async tx => {
        const docs = await tx.getAll(
          ...names.map(name => firestore.doc(apiKeyPath(teamID, name))),
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
