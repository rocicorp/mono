import type {Auth} from 'firebase-admin/auth';
import type {Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {
  createTokenRequestSchema,
  createTokenResponseSchema,
} from 'mirror-protocol/src/token.js';
import {verifyKey} from '../keys/verify.js';
import {validateSchema} from '../validators/schema.js';

export const create = (firestore: Firestore, auth: Auth) =>
  validateSchema(createTokenRequestSchema, createTokenResponseSchema).handle(
    async request => {
      const {key} = request;
      const keyDoc = await verifyKey(firestore, key);
      const keyPath = keyDoc.ref.path;
      logger.info(`Creating custom token for "${keyPath}"`);
      // Note: While we could encode the key's permissions in custom claims of the token,
      // every key authentication looks up the key doc in order to potentially modify
      // the `lastUsed` field, so the permissions check might as well be done then, rather than
      // having to deal with tokens having out-of-date custom claims when permissions change.
      const token = await auth.createCustomToken(keyPath);
      return {success: true, token};
    },
  );
