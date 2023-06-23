import * as v from 'shared/src/valita.js';
import {baseRequestFields, baseResponseFields} from './base.js';

export const moduleSchema = v.object({
  name: v.string(),
  content: v.string(),
  type: v.union(v.literal('esm'), v.literal('text')),
});

export type Module = v.Infer<typeof moduleSchema>;

export const uploadRequestSchema = v.object({
  ...baseRequestFields,
  version: v.string(),
  main: moduleSchema,
  modules: v.array(moduleSchema),
  force: v.boolean().optional(),
});

export type UploadRequest = v.Infer<typeof uploadRequestSchema>;

export const uploadResponseSchema = v.object(baseResponseFields);

export type UploadResponse = v.Infer<typeof uploadResponseSchema>;
