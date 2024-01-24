import type {Firestore} from 'firebase-admin/firestore';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  listApiKeysRequestSchema,
  listApiKeysResponseSchema,
} from 'mirror-protocol/src/api-keys.js';
import {
  listAppKeysRequestSchema,
  listAppKeysResponseSchema,
} from 'mirror-protocol/src/app-keys.js';
import {
  ALL_PERMISSIONS,
  APP_CREATE_PERMISSION,
  apiKeyDataConverter,
  apiKeysCollection,
} from 'mirror-schema/src/api-key.js';
import {appPath} from 'mirror-schema/src/deployment.js';
import {assertString} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import {
  appAuthorization,
  teamAuthorization,
  userAuthorization,
} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';

export const list = (firestore: Firestore) =>
  validateSchema(listApiKeysRequestSchema, listApiKeysResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(teamAuthorization(firestore, ['admin']))
    .handle(async (request, context) => {
      const {show} = request;
      const {teamID} = context;

      const keys = await firestore
        .collection(apiKeysCollection(teamID))
        .orderBy('lastUsed', 'desc')
        .withConverter(apiKeyDataConverter)
        .get();

      // Lookup the "name" field of all referenced appIDs, and create a map from appID to name.
      const appIDs = new Set(keys.docs.map(doc => doc.data().appIDs).flat());
      const apps = await firestore.getAll(
        ...[...appIDs].map(appID => firestore.doc(appPath(appID))),
        {fieldMask: ['name']},
      );
      const appNames = new Map(
        apps.map(appDoc => {
          if (!appDoc.exists) {
            throw new HttpsError(
              'not-found',
              `App ${appDoc.id} no longer exists`,
            );
          }
          const name = appDoc.get('name');
          assertString(name);
          return [appDoc.id, name];
        }),
      );

      return {
        success: true,
        keys: keys.docs.map(doc => {
          const key = doc.data();
          return {
            name: doc.id,
            value: show ? key.value : null,
            permissions: key.permissions,
            createTime: key.created.toMillis(),
            lastUseTime: key.lastUsed?.toMillis() ?? null,
            apps: Object.fromEntries(
              key.appIDs.map(appID => [appID, must(appNames.get(appID))]),
            ),
          };
        }),
        allPermissions: ALL_PERMISSIONS,
      };
    });

// TODO: Decommission and replace with an error to update @rocicorp/reflect
export const listForApp = (firestore: Firestore) =>
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
        doc.data().appIDs.includes(appID),
      );

      // Omit "app:create" from cli versions that don't know how to handle it.
      const legacyPermissions: Record<string, string> = {...ALL_PERMISSIONS};
      delete legacyPermissions[APP_CREATE_PERMISSION];

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
        allPermissions: legacyPermissions,
      };
    });
