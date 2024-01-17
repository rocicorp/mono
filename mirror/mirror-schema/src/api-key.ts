import * as v from 'shared/src/valita.js';
import {isValidAppName} from './app.js';
import {firestoreDataConverter} from './converter.js';
import {appPath} from './deployment.js';
import * as path from './path.js';
import {timestampSchema} from './timestamp.js';

// All permissions must be a default=false boolean.
// This makes any instance forward compatible with new permissions.
const permissionValue = v.boolean().default(false);

export const permissionsSchema = v.object({
  'app:publish': permissionValue,
  'env:modify': permissionValue,
  'rooms:read': permissionValue,
  'rooms:create': permissionValue,
  'rooms:close': permissionValue,
  'rooms:delete': permissionValue,
  'connections:invalidate': permissionValue,
});

export type Permissions = v.Infer<typeof permissionsSchema>;

export const ALL_PERMISSIONS: {[perm in keyof Permissions]: string} = {
  'app:publish': 'authorizes publishing a new server version',
  'env:modify': 'authorizes modifying environment variables',
  'rooms:read': 'authorizes reading room status',
  'rooms:create': 'authorizes creating new rooms',
  'rooms:close': 'authorizes closing rooms',
  'rooms:delete': 'authorizes deleting rooms',
  'connections:invalidate': 'authorizes invalidating connections to rooms',
} as const;

export function defaultPermissions(): Permissions {
  return v.parse({}, permissionsSchema);
}

export function normalizePermissions(perms: unknown): Permissions {
  return v.parse(perms, permissionsSchema);
}

/** Type used by runtime actions to declare the permission required to execute the action. */
export type RequiredPermission = keyof Permissions;

export const apiKeySchema = v.object({
  value: v.string(),
  permissions: permissionsSchema,
  created: timestampSchema,
  lastUsed: timestampSchema.nullable(),
});

export type ApiKey = v.Infer<typeof apiKeySchema>;

export const apiKeyDataConverter = firestoreDataConverter(apiKeySchema);

export const API_KEY_COLLECTION_ID = 'keys';

export function apiKeysCollection(appID: string): string {
  return path.append(appPath(appID), API_KEY_COLLECTION_ID);
}

export function apiKeyPath(appID: string, name: string): string {
  return path.append(apiKeysCollection(appID), name);
}

// Key names are used in Firestore paths. Use the same constraints for
// app names as we do for key names. This makes things consistent and understandable,
// and avoids illegal document names.
// https://firebase.google.com/docs/firestore/quotas#collections_documents_and_fields
export const isValidApiKeyName = isValidAppName;
