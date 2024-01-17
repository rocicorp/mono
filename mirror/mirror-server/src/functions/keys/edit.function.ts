import type {Firestore} from 'firebase-admin/firestore';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  editAppKeyRequestSchema,
  editAppKeyResponseSchema,
} from 'mirror-protocol/src/app-keys.js';
import {appKeyDataConverter, appKeyPath} from 'mirror-schema/src/api-key.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';
import {validatePermissions} from './create.function.js';

export const edit = (firestore: Firestore) =>
  validateSchema(editAppKeyRequestSchema, editAppKeyResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore, ['admin']))
    .handle(async request => {
      const {appID, name, permissions} = request;

      const validatedPermissions = validatePermissions(name, permissions);
      const keyDoc = firestore
        .doc(appKeyPath(appID, name))
        .withConverter(appKeyDataConverter);

      await firestore.runTransaction(async tx => {
        const doc = await tx.get(keyDoc);
        if (!doc.exists) {
          throw new HttpsError(
            'not-found',
            `Key named "${name}" was not found.`,
          );
        }
        tx.update(keyDoc, {permissions: validatedPermissions});
      });

      return {success: true};
    });
