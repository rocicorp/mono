import * as v from 'shared/src/valita.js';
import {baseAppRequestFields} from './app.js';
import {baseResponseFields} from './base.js';
import {createCaller} from './call.js';
export {isValidAppKeyName} from 'mirror-schema/src/app-key.js';

// Unlike the Permissions type in `mirror-schema`, the type used in the network protocol
// only declares the "shape" of the permissions object without specifying the keys.
// This allows the client to be agnostic to the actual set of permissions (in other words,
// forwards compatible with new permissions).
export const permissionsSchema = v.record(v.boolean());

export const appKeySchema = v.object({
  name: v.string(),
  value: v.string().nullable(), // Null if `show` was not requested (required admin privileges)
  permissions: permissionsSchema,
  createTime: v.number(),
  lastUseTime: v.number().nullable(),
});

export const listAppKeysRequestSchema = v.object({
  ...baseAppRequestFields,

  show: v.boolean(),
});

export const listAppKeysResponseSchema = v.object({
  ...baseResponseFields,

  keys: v.array(appKeySchema),

  // Ordered mapping of all permissions to an optional description.
  allPermissions: v.record(v.string()),
});

export type ListAppKeysRequest = v.Infer<typeof listAppKeysRequestSchema>;
export type ListAppKeysResponse = v.Infer<typeof listAppKeysResponseSchema>;

export const createAppKeyRequestSchema = v.object({
  ...baseAppRequestFields,

  name: v.string(),
  permissions: permissionsSchema,
});

export const createAppKeyResponseSchema = v.object({
  ...baseResponseFields,

  value: v.string(),
});

export type CreateAppKeyRequest = v.Infer<typeof createAppKeyRequestSchema>;
export type CreateAppKeyResponse = v.Infer<typeof createAppKeyResponseSchema>;

export const editAppKeyRequestSchema = v.object({
  ...baseAppRequestFields,

  name: v.string(),
  permissions: permissionsSchema,
});

export const editAppKeyResponseSchema = v.object({
  ...baseResponseFields,
});

export type EditAppKeyRequest = v.Infer<typeof editAppKeyRequestSchema>;
export type EditAppKeyResponse = v.Infer<typeof editAppKeyResponseSchema>;

export const deleteAppKeysRequestSchema = v.object({
  ...baseAppRequestFields,

  // Names of app keys to delete. Non-existent keys are ignored.
  names: v.array(v.string()),
});

export const deleteAppKeysResponseSchema = v.object({
  ...baseResponseFields,

  deleted: v.array(v.string()),
});

export type DeleteAppKeysRequest = v.Infer<typeof deleteAppKeysRequestSchema>;
export type DeleteAppKeysResponse = v.Infer<typeof deleteAppKeysResponseSchema>;

export const listAppKeys = createCaller(
  'appKeys-list',
  listAppKeysRequestSchema,
  listAppKeysResponseSchema,
);

export const createAppKey = createCaller(
  'appKeys-create',
  createAppKeyRequestSchema,
  createAppKeyResponseSchema,
);

export const editAppKey = createCaller(
  'appKeys-edit',
  editAppKeyRequestSchema,
  editAppKeyResponseSchema,
);

export const deleteAppKeys = createCaller(
  'appKeys-delete',
  deleteAppKeysRequestSchema,
  deleteAppKeysResponseSchema,
);
