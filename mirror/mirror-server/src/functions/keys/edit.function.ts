import type {Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  editAppKeyRequestSchema,
  editAppKeyResponseSchema,
} from 'mirror-protocol/src/app-keys.js';
import {
  appKeyDataConverter,
  appKeyPath,
  normalizePermissions,
  type Permissions,
} from 'mirror-schema/src/app-key.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';

export const edit = (firestore: Firestore) =>
  validateSchema(editAppKeyRequestSchema, editAppKeyResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore, ['admin']))
    .handle(async request => {
      const {appID, name, permissions} = request;

      let validatedPermissions: Permissions;
      try {
        validatedPermissions = normalizePermissions(permissions);
      } catch (e) {
        logger.warn(`Rejecting permissions: ${String(e)}`, permissions);
        throw new HttpsError('invalid-argument', 'Invalid permissions');
      }

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
