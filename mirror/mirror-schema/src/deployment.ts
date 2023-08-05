import * as v from 'shared/src/valita.js';
import {appPath} from './app.js';
import {firestoreDataConverter} from './converter.js';
import * as path from './path.js';
import {timestampSchema} from './timestamp.js';
import {moduleRefSchema} from './module.js';

export const deploymentTypeSchema = v.union(
  v.literal('USER_UPLOAD'),
  v.literal('USER_ROLLBACK'),
  v.literal('SERVER_UPDATE'),
  v.literal('SERVER_ROLLBACK'),
);
export type DeploymentType = v.Infer<typeof deploymentTypeSchema>;

export const deploymentStatusSchema = v.union(
  v.literal('REQUESTED'),
  v.literal('DEPLOYING'),
  v.literal('RUNNING'),
  v.literal('STOPPED'),
  v.literal('FAILED'),
);
export type DeploymentStatus = v.Infer<typeof deploymentStatusSchema>;

export const deploymentSchema = v.object({
  requesterID: v.string(), // userID
  type: deploymentTypeSchema,
  // The first app module must be the "main" module that exports the
  // ReflectServerOptions creation function as default.
  appModules: v.array(moduleRefSchema),
  // The hostname of the worker, which is https://<appName>.reflect-server.net
  // can be a vanity domain in the future.
  hostname: v.string(),
  appVersion: v.string().optional(),
  description: v.string().optional(),
  serverVersionRange: v.string(),
  serverVersion: v.string(),
  status: deploymentStatusSchema,
  // A message associated with the current status of the deployment that is
  // suitable for displaying to the user.
  statusMessage: v.string().optional(),

  requestTime: timestampSchema,
  deployTime: timestampSchema.optional(),
  startTime: timestampSchema.optional(),
  stopTime: timestampSchema.optional(),
});

export type Deployment = v.Infer<typeof deploymentSchema>;

export const deploymentDataConverter = firestoreDataConverter(deploymentSchema);

export const APP_DEPLOYMENTS_COLLECTION_ID = 'deployments';

export function deploymentsCollection(appID: string): string {
  return path.append(appPath(appID), APP_DEPLOYMENTS_COLLECTION_ID);
}

export function deploymentPath(appID: string, deploymentID: string): string {
  return path.append(deploymentsCollection(appID), deploymentID);
}
