import * as v from 'shared/src/valita.js';
import {baseResponseFields} from './base.js';
import {baseAppRequestFields} from './app.js';
import {createCall} from './call.js';

const fileSchema = v.object({
  content: v.string(),
  name: v.string(),
});

export const publishRequestSchema = v.object({
  ...baseAppRequestFields,
  source: fileSchema,
  sourcemap: fileSchema,
  serverVersionRange: v.string(),

  // Sets the App's server release channel when present.
  serverReleaseChannel: v.string().optional(),
});

export type PublishRequest = v.Infer<typeof publishRequestSchema>;

export const publishResponseSchema = v.object({
  ...baseResponseFields,
  deploymentPath: v.string(),
});
export type PublishResponse = v.Infer<typeof publishResponseSchema>;

export const publish = createCall(
  'app-publish',
  publishRequestSchema,
  publishResponseSchema,
);
