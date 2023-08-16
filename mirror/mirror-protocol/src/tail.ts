import * as v from 'shared/src/valita.js';
import {baseRequestFields, baseResponseFields} from './base.js';
import {createCall} from './call.js';
import {baseAppRequestFields} from './app.js';
import {createEventSource} from './event-source.js';
import type EventSource from 'eventsource';

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

export const createTailRequestSchema = v.object({
  ...baseAppRequestFields,
});

export type CreateTailRequest = v.Infer<typeof createTailRequestSchema>;

export const createTailResponseSchema = v.object({
  ...baseResponseFields,
});
export type CreateTailResponse = v.Infer<typeof createTailResponseSchema>;

export const createTail = (
  appID: string,
  idToken: string,
  data: CreateTailRequest,
): EventSource => createEventSource('tail-create', appID, idToken, data);
