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
});

export type PublishRequest = v.Infer<typeof publishRequestSchema>;

export const publishResponseSchema = v.object({
  ...baseResponseFields,
  hostname: v.string(),
});
export type PublishResponse = v.Infer<typeof publishResponseSchema>;

export const publish = createCall(
  'publish',
  publishRequestSchema,
  publishResponseSchema,
);
