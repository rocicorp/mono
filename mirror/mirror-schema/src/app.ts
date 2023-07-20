import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import * as path from './path.js';
import {releaseChannelSchema} from './server.js';

export const appSchema = v.object({
  cfID: v.string(),
  // Globally unique, stable, internal script name in Cloudflare.
  cfScriptName: v.string(),
  // The user requested name, which must be suitable as a subdomain
  // (lower-cased alphanumeric with hyphens). Uniqueness is enforced
  // by the APP_NAME_INDEX_COLLECTION. The app worker URL is
  // https://<name>.reflect-server.net/.
  //
  // Users can rename their app (and thus worker url) via the
  // app-rename command.
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

export const appNameIndexSchema = v.object({
  appID: v.string(),
});

export type AppNameIndex = v.Infer<typeof appNameIndexSchema>;

export const appNameIndexDataConverter =
  firestoreDataConverter(appNameIndexSchema);

export const APP_NAME_INDEX_COLLECTION = 'appNames';

export function appNameIndexPath(appName: string): string {
  return path.join(APP_NAME_INDEX_COLLECTION, appName);
}

const VALID_APP_NAME = /^[a-z]([a-z0-9\\-])*[a-z0-9]$/;

export function isValidAppName(name: string): boolean {
  return VALID_APP_NAME.test(name);
}
