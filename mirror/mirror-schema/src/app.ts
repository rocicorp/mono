import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import * as path from './path.js';
import {releaseChannelSchema} from './server.js';

export const appSchema = v.object({
  cfID: v.string(),
  cfScriptName: v.string(),
  name: v.string(),
  serverReleaseChannel: releaseChannelSchema,
  teamID: v.string(),
});

export type App = v.Infer<typeof appSchema>;

export const appDataConverter = firestoreDataConverter(appSchema);

export const APP_COLLECTION = 'apps';

export function appPath(appID: string): string {
  return path.join(APP_COLLECTION, appID);
}
