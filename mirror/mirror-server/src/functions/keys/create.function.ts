import {FieldValue, type Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  createAppKeyRequestSchema,
  createAppKeyResponseSchema,
} from 'mirror-protocol/src/app-keys.js';
import {
  apiKeyDataConverter,
  apiKeyPath,
  apiKeysCollection,
  isValidApiKeyName,
  normalizePermissions,
  type Permissions,
} from 'mirror-schema/src/api-key.js';
import {randomBytes} from 'node:crypto';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
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
  validateSchema(createAppKeyRequestSchema, createAppKeyResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore, ['admin']))
    .handle(async (request, context) => {
      const {appID, name, permissions} = request;
      const {
        app: {teamID},
      } = context;

      if (!isValidApiKeyName(name)) {
        throw new HttpsError(
          'invalid-argument',
          `Invalid name "${name}". Names must be lowercased alphanumeric, starting with a letter and not ending with a hyphen.`,
        );
      }
      const validatedPermissions = validatePermissions(name, permissions);

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
          apps: [appID],
        });
      });

      return {
        success: true,
        value,
      };
    });
