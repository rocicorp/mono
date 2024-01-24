import {FieldValue, type Firestore} from 'firebase-admin/firestore';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  editApiKeyRequestSchema,
  editApiKeyResponseSchema,
  type EditApiKeyResponse,
} from 'mirror-protocol/src/api-keys.js';
import {
  editAppKeyRequestSchema,
  editAppKeyResponseSchema,
} from 'mirror-protocol/src/app-keys.js';
import {
  APP_CREATE_PERMISSION,
  apiKeyDataConverter,
  apiKeyPath,
} from 'mirror-schema/src/api-key.js';
import {must} from 'shared/src/must.js';
import {
  appAuthorization,
  teamAuthorization,
  userAuthorization,
} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';
import {validatePermissions} from './create.function.js';

export const edit = (firestore: Firestore) =>
  validateSchema(editApiKeyRequestSchema, editApiKeyResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(teamAuthorization(firestore, ['admin']))
    .handle(request => {
      const {teamID, name, permissions, appIDs} = request;
      return editKeys(firestore, teamID, name, permissions, appIDs);
    });

// TODO: Decommission and replace with an error to update @rocicorp/reflect
export const editForApp = (firestore: Firestore) =>
  validateSchema(editAppKeyRequestSchema, editAppKeyResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore, ['admin']))
    .handle((request, context) => {
      const {name, permissions} = request;
      const {
        app: {teamID},
      } = context;
      return editKeys(firestore, teamID, name, permissions, {
        add: [],
        remove: [],
      });
    });

async function editKeys(
  firestore: Firestore,
  teamID: string,
  name: string,
  permissions: Record<string, boolean>,
  appIDs: {add: string[]; remove: string[]},
): Promise<EditApiKeyResponse> {
  // Sanity check arguments
  const validatedPermissions = validatePermissions(name, permissions);
  const remove = new Set(appIDs.remove);
  appIDs.add.forEach(id => {
    if (remove.has(id)) {
      throw new HttpsError(
        'invalid-argument',
        `AppID ${id} cannot be both added and removed`,
      );
    }
  });

  const keyDoc = firestore
    .doc(apiKeyPath(teamID, name))
    .withConverter(apiKeyDataConverter);

  await firestore.runTransaction(async tx => {
    const doc = await tx.get(keyDoc);
    if (!doc.exists) {
      throw new HttpsError('not-found', `Key named "${name}" was not found.`);
    }
    const key = must(doc.data());
    if (
      !permissions[APP_CREATE_PERMISSION] &&
      appIDs.add.length === 0 &&
      !key.appIDs.some(id => !remove.has(id))
    ) {
      throw new HttpsError('invalid-argument', 'No authorized apps specified');
    }

    tx.update(keyDoc, {permissions: validatedPermissions});
    if (appIDs.add.length) {
      tx.update(keyDoc, {appIDs: FieldValue.arrayUnion(...appIDs.add)});
    }
    if (appIDs.remove.length) {
      tx.update(keyDoc, {appIDs: FieldValue.arrayRemove(...appIDs.remove)});
    }
  });

  return {success: true};
}
