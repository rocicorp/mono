import {releaseChannelSchema} from 'mirror-schema/src/server.js';
import * as v from 'shared/src/valita.js';
import {baseRequestFields, baseResponseFields} from './base.js';
import {createCall} from './call.js';

export const createRequestSchema = v.object({
  ...baseRequestFields,
  serverReleaseChannel: releaseChannelSchema,
});

export type CreateRequest = v.Infer<typeof createRequestSchema>;

export const createResponseSchema = v.object({
  ...baseResponseFields,
  appID: v.string(),
  name: v.string(),
});
export type CreateResponse = v.Infer<typeof createResponseSchema>;

export const create = createCall(
  'create',
  createRequestSchema,
  createResponseSchema,
);
