import {FieldValue, type Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  CreateApiKeyResponse,
  createApiKeyRequestSchema,
  createApiKeyResponseSchema,
} from 'mirror-protocol/src/api-keys.js';
import {
  createAppKeyRequestSchema,
  createAppKeyResponseSchema,
} from 'mirror-protocol/src/app-keys.js';
import {
  APP_CREATE_PERMISSION,
  apiKeyDataConverter,
  apiKeyPath,
  apiKeysCollection,
  isValidApiKeyName,
  normalizePermissions,
  type Permissions,
} from 'mirror-schema/src/api-key.js';
import {randomBytes} from 'node:crypto';
import {
  appAuthorization,
  teamAuthorization,
  userAuthorization,
} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';

// TODO: Revisit this limit since it is now per team instead of per app.
export const MAX_KEYS = 100;

export function validatePermissions(
  keyName: string,
  permissions: Record<string, boolean>,
): Permissions {
  if (Object.keys(permissions).length === 0) {
    throw new HttpsError(
      'invalid-argument',
      `No permissions specified for key named "${keyName}"`,
    );
  }
  try {
    return normalizePermissions(permissions);
  } catch (e) {
    logger.warn(`Rejecting permissions: ${String(e)}`, permissions);
    throw new HttpsError('invalid-argument', 'Invalid permissions');
  }
}

export const create = (firestore: Firestore) =>
  validateSchema(createApiKeyRequestSchema, createApiKeyResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(teamAuthorization(firestore, ['admin']))
    .handle(request => {
      const {teamID, name, permissions, appIDs} = request;

      return createKey(firestore, teamID, name, permissions, appIDs);
    });

// TODO: Decommission and replace with an error to update @rocicorp/reflect
export const createForApp = (firestore: Firestore) =>
  validateSchema(createAppKeyRequestSchema, createAppKeyResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore, ['admin']))
    .handle((request, context) => {
      const {appID, name, permissions} = request;
      const {
        app: {teamID},
      } = context;

      return createKey(firestore, teamID, name, permissions, [appID]);
    });

async function createKey(
  firestore: Firestore,
  teamID: string,
  name: string,
  permissions: Record<string, boolean>,
  appIDs: string[],
): Promise<CreateApiKeyResponse> {
  if (!isValidApiKeyName(name)) {
    throw new HttpsError(
      'invalid-argument',
      `Invalid name "${name}". Names must be lowercased alphanumeric, starting with a letter and not ending with a hyphen.`,
    );
  }
  const validatedPermissions = validatePermissions(name, permissions);

  if (!permissions[APP_CREATE_PERMISSION] && appIDs.length === 0) {
    throw new HttpsError('invalid-argument', 'No authorized apps specified');
  }

  const keyDoc = firestore
    .doc(apiKeyPath(teamID, name))
    .withConverter(apiKeyDataConverter);

  const value = randomBytes(32).toString('base64url');
  await firestore.runTransaction(async tx => {
    const keys = await tx.get(
      firestore.collection(apiKeysCollection(teamID)).count(),
    );
    if (keys.data().count >= MAX_KEYS) {
      throw new HttpsError(
        'resource-exhausted',
        'Maximum keys reached. Use `reflect keys delete` to delete keys',
      );
    }
    const doc = await tx.get(keyDoc);
    if (doc.exists) {
      throw new HttpsError(
        'already-exists',
        `A key named "${name}" already exists.`,
      );
    }
    tx.create(keyDoc, {
      value,
      permissions: validatedPermissions,
      created: FieldValue.serverTimestamp(),
      lastUsed: null,
      appIDs,
    });
  });

  return {
    success: true,
    value,
  };
}
