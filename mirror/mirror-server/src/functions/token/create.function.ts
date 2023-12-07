import type {Auth} from 'firebase-admin/auth';
import type {Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  createTokenRequestSchema,
  createTokenResponseSchema,
} from 'mirror-protocol/src/token.js';
import {
  APP_KEY_COLLECTION_ID,
  appKeyDataConverter,
} from 'mirror-schema/src/app-key.js';
import {validateSchema} from '../validators/schema.js';

export const create = (firestore: Firestore, auth: Auth) =>
  validateSchema(createTokenRequestSchema, createTokenResponseSchema).handle(
    async request => {
      const {key} = request;

      const query = await firestore
        .collectionGroup(APP_KEY_COLLECTION_ID)
        .withConverter(appKeyDataConverter)
        .where('value', '==', key)
        .get();
      if (query.size === 0) {
        throw new HttpsError('permission-denied', `Invalid key`);
      }
      if (query.size > 1) {
        throw new HttpsError(
          'failed-precondition',
          `Multiple keys matched.`, // Should never happen (unless random is broken), but fail to be safe.
        );
      }
      const keyDoc = query.docs[0];
      const {
        ref: {path: keyPath},
      } = keyDoc;
      logger.info(`Creating custom token for "${keyPath}"`);
      // Note: While we could encode the key's permissions in custom claims of the token,
      // every key authentication looks up the key doc in order to potentially modify
      // the `lastUsed` field, so the permissions check might as well be done then, rather than
      // having to deal with tokens having out-of-date custom claims when permissions change.
      const token = await auth.createCustomToken(keyPath);
      return {success: true, token};
    },
  );
