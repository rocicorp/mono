import * as v from 'shared/src/valita.js';
import {baseResponseFields} from './base.js';
import {createCaller} from './call.js';
import {baseTeamRequestFields} from './team.js';
export {isValidApiKeyName} from 'mirror-schema/src/api-key.js';

// Unlike the Permissions type in `mirror-schema`, the type used in the network protocol
// only declares the "shape" of the permissions object without specifying the keys.
// This allows the client to be agnostic to the actual set of permissions (in other words,
// forwards compatible with new permissions).
export const permissionsSchema = v.record(v.boolean());

export const apiKeySchema = v.object({
  name: v.string(),
  value: v.string().nullable(), // Null if `show` was not requested (required admin privileges)
  permissions: permissionsSchema,
  createTime: v.number(),
  lastUseTime: v.number().nullable(),

  // Record<appID, appName>
  apps: v.record(v.string()),
});

export const listApiKeysRequestSchema = v.object({
  ...baseTeamRequestFields,

  show: v.boolean(),
});

export const listApiKeysResponseSchema = v.object({
  ...baseResponseFields,

  keys: v.array(apiKeySchema),

  // Ordered mapping of all permissions to an optional description.
  allPermissions: v.record(v.string()),
});

export type ListApiKeysRequest = v.Infer<typeof listApiKeysRequestSchema>;
export type ListApiKeysResponse = v.Infer<typeof listApiKeysResponseSchema>;

export const createApiKeyRequestSchema = v.object({
  ...baseTeamRequestFields,

  name: v.string(),
  permissions: permissionsSchema,
  appIDs: v.array(v.string()),
});

export const createApiKeyResponseSchema = v.object({
  ...baseResponseFields,

  value: v.string(),
});

export type CreateApiKeyRequest = v.Infer<typeof createApiKeyRequestSchema>;
export type CreateApiKeyResponse = v.Infer<typeof createApiKeyResponseSchema>;

export const editApiKeyRequestSchema = v.object({
  ...baseTeamRequestFields,

  name: v.string(),

  // The desired permissions for the key. This must be specified even if there are no changes.
  permissions: permissionsSchema,

  // AppIDs to add or remove. This is represented as a diff rather than the desired set
  // in order to avoid trampling over automated changes to appIDs (e.g. when the key is used to
  // create a new app, the appID is added to the array).
  appIDs: v.object({
    add: v.array(v.string()),
    remove: v.array(v.string()),
  }),
});

export const editApiKeyResponseSchema = v.object({
  ...baseResponseFields,
});

export type EditApiKeyRequest = v.Infer<typeof editApiKeyRequestSchema>;
export type EditApiKeyResponse = v.Infer<typeof editApiKeyResponseSchema>;

export const deleteApiKeysRequestSchema = v.object({
  ...baseTeamRequestFields,

  // Names of app keys to delete. Non-existent keys are ignored.
  names: v.array(v.string()),
});

export const deleteApiKeysResponseSchema = v.object({
  ...baseResponseFields,

  deleted: v.array(v.string()),
});

export type DeleteApiKeysRequest = v.Infer<typeof deleteApiKeysRequestSchema>;
export type DeleteApiKeysResponse = v.Infer<typeof deleteApiKeysResponseSchema>;

export const listApiKeys = createCaller(
  'apiKeys-list',
  listApiKeysRequestSchema,
  listApiKeysResponseSchema,
);

export const createApiKey = createCaller(
  'apiKeys-create',
  createApiKeyRequestSchema,
  createApiKeyResponseSchema,
);

export const editApiKey = createCaller(
  'apiKeys-edit',
  editApiKeyRequestSchema,
  editApiKeyResponseSchema,
);

export const deleteApiKeys = createCaller(
  'apiKeys-delete',
  deleteApiKeysRequestSchema,
  deleteApiKeysResponseSchema,
);
