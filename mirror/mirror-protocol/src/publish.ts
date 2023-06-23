import * as v from 'shared/src/valita.js';
import {baseRequestFields, baseResponseFields} from './base.js';

const fileSchema = v.object({
  content: v.string(),
  name: v.string(),
});

export const publishRequestSchema = v.object({
  ...baseRequestFields,
  /** The name of the Reflect App */
  name: v.string(),
  source: fileSchema,
  sourcemap: fileSchema,
  desiredVersion: v.string(),
});

export type PublishRequest = v.Infer<typeof publishRequestSchema>;

export const publishResponseSchema = v.object(baseResponseFields);
export type PublishResponse = v.Infer<typeof publishResponseSchema>;
