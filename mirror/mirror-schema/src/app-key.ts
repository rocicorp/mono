import * as v from 'shared/src/valita.js';
import {isValidAppName} from './app.js';
import {firestoreDataConverter} from './converter.js';
import {appPath} from './deployment.js';
import * as path from './path.js';
import {timestampSchema} from './timestamp.js';

function defaultFalse() {
  return v.boolean().default(false);
}

export const permissionsSchema = v.object({
  'app:publish': defaultFalse(),
  'rooms:read': defaultFalse(),
  'rooms:create': defaultFalse(),
  'rooms:close': defaultFalse(),
  'rooms:delete': defaultFalse(),
  'connections:invalidate': defaultFalse(),
});

export type Permissions = v.Infer<typeof permissionsSchema>;

export const ALL_PERMISSIONS: {[perm in keyof Permissions]: string} = {
  'app:publish': 'authorizes `npx reflect publish`',
  'rooms:read': 'REST API coming soon',
  'rooms:create': 'REST API coming soon',
  'rooms:close': 'REST API coming soon',
  'rooms:delete': 'REST API coming soon',
  'connections:invalidate': 'REST API coming soon',
} as const;

export function defaultPermissions(): Permissions {
  return v.parse({}, permissionsSchema);
}

export function normalizePermissions(perms: unknown): Permissions {
  return v.parse(perms, permissionsSchema);
}

/** Type used by runtime actions to declare the permission required to execute the action. */
export type RequiredPermission = keyof Permissions;

export const appKeySchema = v.object({
  value: v.string(),
  permissions: permissionsSchema,
  created: timestampSchema,
  lastUsed: timestampSchema.nullable(),
});

export type AppKey = v.Infer<typeof appKeySchema>;

export const appKeyDataConverter = firestoreDataConverter(appKeySchema);

export const APP_KEY_COLLECTION_ID = 'keys';

export function appKeysCollection(appID: string): string {
  return path.append(appPath(appID), APP_KEY_COLLECTION_ID);
}

export function appKeyPath(appID: string, name: string): string {
  return path.append(appKeysCollection(appID), name);
}

// Key names are used in Firestore paths. Use the same constraints for
// app names as we do for key names. This makes things consistent and understandable,
// and avoids illegal document names.
// https://firebase.google.com/docs/firestore/quotas#collections_documents_and_fields
export const isValidAppKeyName = isValidAppName;
