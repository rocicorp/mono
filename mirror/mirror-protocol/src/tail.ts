import * as v from 'shared/src/valita.js';
import {baseRequestFields, baseResponseFields} from './base.js';
import {createCall} from './call.js';

export const deleteTailRequestSchema = v.object({
  ...baseRequestFields,
  appID: v.string(),
  tailID: v.string(),
  env: v.string().optional(),
});
export type DeleteTailRequest = v.Infer<typeof deleteTailRequestSchema>;

export const deleteTailResponseSchema = v.object({
  ...baseResponseFields,
});
export type DeleteTailResponse = v.Infer<typeof deleteTailResponseSchema>;

export const deleteTail = createCall(
  'tail-delete',
  deleteTailRequestSchema,
  deleteTailResponseSchema,
);
