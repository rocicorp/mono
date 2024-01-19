import type {Firestore} from 'firebase-admin/firestore';
import {
  listAppKeysRequestSchema,
  listAppKeysResponseSchema,
} from 'mirror-protocol/src/app-keys.js';
import {
  ALL_PERMISSIONS,
  apiKeyDataConverter,
  apiKeysCollection,
} from 'mirror-schema/src/api-key.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';

export const list = (firestore: Firestore) =>
  validateSchema(listAppKeysRequestSchema, listAppKeysResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore, ['admin']))
    .handle(async (request, context) => {
      const {appID, show} = request;
      const {
        app: {teamID},
      } = context;

      const keys = await firestore
        .collection(apiKeysCollection(teamID))
        .orderBy('lastUsed', 'desc')
        .withConverter(apiKeyDataConverter)
        .get();

      // For backwards compatibility, only list the keys for the specified `appID`.
      const appKeyDocs = keys.docs.filter(doc =>
        doc.data().apps.includes(appID),
      );

      return {
        success: true,
        keys: appKeyDocs.map(doc => {
          const key = doc.data();
          return {
            name: doc.id,
            value: show ? key.value : null,
            permissions: key.permissions,
            createTime: key.created.toMillis(),
            lastUseTime: key.lastUsed?.toMillis() ?? null,
          };
        }),
        allPermissions: ALL_PERMISSIONS,
      };
    });
