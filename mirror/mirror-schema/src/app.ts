import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import {deploymentOptionsSchema, deploymentSchema} from './deployment.js';
import * as path from './path.js';

export const appSchema = v.object({
  cfID: v.string(),
  // Globally unique, stable, internal script name in Cloudflare.
  cfScriptName: v.string(),
  teamID: v.string(),

  // The user requested name, which must be suitable as a subdomain
  // (lower-cased alphanumeric with hyphens). Uniqueness is enforced
  // by the APP_NAME_INDEX_COLLECTION. The app worker URL is
  // https://<name>.reflect-server.net/.
  //
  // Users can rename their app (and thus worker url) via the
  // app-rename command.
  name: v.string(),

  // The release channel from which server versions are chosen.
  //
  // Apps can only be created with a `StandardReleaseChannel` (i.e. "canary" and "stable",
  // type-restricted via the app.CreateRequest schema), but the App schema itself
  // allows for custom channels to be arbitrarily created/used for pushing builds
  // to particular apps or sets of them. Note that custom channels should be used
  // sparingly and temporarily, as they run the risk of being missed in the standard
  // release process.
  serverReleaseChannel: v.string(),

  deploymentOptions: deploymentOptionsSchema,

  // The App document tracks the running and queued deployments and serves as
  // a coordination point for (1) determining if a new deployment is necessary
  // (i.e. if the desired `DeploymentSpec` differs from that which is running)
  // and (2) ensuring that deployments are executed in their requested order.
  //
  // These fields are transactionally consistent views of the documents in the
  // deployments subcollection.
  runningDeployment: deploymentSchema.optional(),
  queuedDeploymentIDs: v.array(v.string()).optional(),
});

export type App = v.Infer<typeof appSchema>;

export const appDataConverter = firestoreDataConverter(appSchema);

// APP_COLLECTION and appPath() are defined in deployment.js to avoid a cyclic
// dependency (which otherwise breaks mjs targets). Re-export them here to be
// consistent with other schema files.
export {APP_COLLECTION, appPath} from './deployment.js';

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
