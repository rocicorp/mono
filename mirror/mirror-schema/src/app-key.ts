import * as v from 'shared/valita.js';
import * as path from './path.js';
import {timestampSchema} from './timestamp.js';
import {firestoreDataConverter} from './converter.js';

export const appKeySchema = v.object({
  appID: v.string(),
  userID: v.string(),
  issued: timestampSchema,
});

export type AppKey = v.Infer<typeof appKeySchema>;

export const appKeyDataConverter = firestoreDataConverter(appKeySchema);

export const APP_KEY_COLLECTION = 'appKeys';

export function appKeyPath(appKey: string): string {
  return path.join(APP_KEY_COLLECTION, appKey);
}
