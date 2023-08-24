import * as v from 'shared/src/valita.js';
import {baseResponseFields} from './base.js';
import {baseAppRequestFields} from './app.js';
import {createEventSource} from './event-source.js';
import type EventSource from 'eventsource';
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
