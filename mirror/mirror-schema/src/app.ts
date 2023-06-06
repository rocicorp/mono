import * as v from 'shared/valita.js';
import * as path from './path.js';
import {releaseChannelSchema} from './server.js';

export const appSchema = v.object({
  teamID: v.string(),
  name: v.string(),
  cfID: v.string(),
  cfScriptName: v.string(),
  serverReleaseChannel: releaseChannelSchema,
});

export type App = v.Infer<typeof appSchema>;

export const APP_COLLECTION = 'apps';

export function appPath(appID: string): string {
  return path.join(APP_COLLECTION, appID);
}
