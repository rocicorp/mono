import * as v from 'shared/src/valita.js';
import {baseResponseFields} from './base.js';
import {baseAppRequestFields} from './app.js';
import {createEventSource, TailEventSource} from './event-source.js';

export const tailRequestSchema = v.object(baseAppRequestFields);

export type TailRequest = v.Infer<typeof tailRequestSchema>;

export const tailResponseSchema = v.object(baseResponseFields);

export type TailResponse = v.Infer<typeof tailResponseSchema>;

export const tail = (
  appID: string,
  idToken: string,
  data: TailRequest,
): Promise<TailEventSource> =>
  createEventSource('app-tail', appID, idToken, data);
