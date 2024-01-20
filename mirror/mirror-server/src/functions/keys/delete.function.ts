import type {Firestore} from 'firebase-admin/firestore';
import {
  DeleteApiKeysResponse,
  deleteApiKeysRequestSchema,
  deleteApiKeysResponseSchema,
} from 'mirror-protocol/src/api-keys.js';
import {
  deleteAppKeysRequestSchema,
  deleteAppKeysResponseSchema,
} from 'mirror-protocol/src/app-keys.js';
import {apiKeyPath} from 'mirror-schema/src/api-key.js';
import {
  appAuthorization,
  teamAuthorization,
  userAuthorization,
} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';

export const deleteFn = (firestore: Firestore) =>
  validateSchema(deleteApiKeysRequestSchema, deleteApiKeysResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(teamAuthorization(firestore, ['admin']))
    .handle(request => {
      const {teamID, names} = request;
      return deleteKeys(firestore, teamID, names);
    });

// TODO: Decommission and replace with an error to update @rocicorp/reflect
export const deleteForApp = (firestore: Firestore) =>
  validateSchema(deleteAppKeysRequestSchema, deleteAppKeysResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore, ['admin']))
    .handle((request, context) => {
      const {names} = request;
      const {
        app: {teamID},
      } = context;

      return deleteKeys(firestore, teamID, names);
    });

async function deleteKeys(
  firestore: Firestore,
  teamID: string,
  names: string[],
): Promise<DeleteApiKeysResponse> {
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
}
