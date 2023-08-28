import {getFunctions, type Functions} from 'firebase/functions';
import * as v from 'shared/src/valita.js';
import {baseAppRequestFields} from './app.js';
import {baseResponseFields} from './base.js';

export const tailRequestSchema = v.object(baseAppRequestFields);

export type TailRequest = v.Infer<typeof tailRequestSchema>;

export const tailResponseSchema = v.object(baseResponseFields);

export type TailResponse = v.Infer<typeof tailResponseSchema>;

export function createTailEventSourceURL(
  functionName: string,
  appID: string,
): string {
  const functions: Functions & {emulatorOrigin?: string} = getFunctions();
  if (functions.emulatorOrigin) {
    return `${functions.emulatorOrigin}/${functions.app.options.projectId}/${functions.region}/${functionName}/${appID}`;
  }
  return `https://${functions.region}-${functions.app.options.projectId}.cloudfunctions.net/${functionName}/${appID}`;
}
