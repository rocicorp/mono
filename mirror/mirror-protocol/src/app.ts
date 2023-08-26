import {standardReleaseChannelSchema} from 'mirror-schema/src/server.js';
import * as v from 'shared/src/valita.js';
import {baseRequestFields, baseResponseFields} from './base.js';
import {createCall} from './call.js';

export const createRequestSchema = v.object({
  ...baseRequestFields,
  serverReleaseChannel: standardReleaseChannelSchema,
  name: v.string().optional(),
});

export type CreateRequest = v.Infer<typeof createRequestSchema>;

export const createResponseSchema = v.object({
  ...baseResponseFields,
  appID: v.string(),
  name: v.string(),
});
export type CreateResponse = v.Infer<typeof createResponseSchema>;

export const create = createCall(
  'app-create',
  createRequestSchema,
  createResponseSchema,
);

export const baseAppRequestFields = {
  ...baseRequestFields,
  appID: v.string(),
};

export const baseAppRequestSchema = v.object(baseAppRequestFields);
export type BaseAppRequest = v.Infer<typeof baseAppRequestSchema>;

export const renameAppRequestSchema = v.object({
  ...baseAppRequestFields,
  name: v.string(),
});

export type RenameAppRequest = v.Infer<typeof renameAppRequestSchema>;

export const renameAppResponseSchema = v.object({
  ...baseResponseFields,
});
export type RenameAppResponse = v.Infer<typeof renameAppResponseSchema>;

export const rename = createCall(
  'app-rename',
  renameAppRequestSchema,
  renameAppResponseSchema,
);
