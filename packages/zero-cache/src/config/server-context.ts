import {hostname} from 'node:os';
import {getHostIp} from './network.ts';
import type {NormalizedZeroConfig} from './normalize.ts';

export type ServerContext = {
  appID: string;
  taskID: string;
  hostIP: string;
  hostname: string;
  timestamp: string;

  [platformVar: string]: string | undefined;
};

/**
 * Runtime and environment information useful for recording and identifying
 * the origin of certain actions such as initial sync and logical replication
 * takeovers.
 */
export function getServerContext(config: NormalizedZeroConfig): ServerContext {
  const context: ServerContext = {
    appID: config.app.id,
    taskID: config.taskID,
    hostIP: getHostIp(),
    hostname: hostname(),
    timestamp: new Date().toISOString(),

    // Platform-specific environment variables
    flyAppName: process.env.FLY_APP_NAME,
    flyMachineID: process.env.FLY_MACHINE_ID,

    railwayProjectID: process.env.RAILWAY_PROJECT_ID,
    railwayProjectName: process.env.RAILWAY_PROJECT_NAME,
    railwayEnvironmentID: process.env.RAILWAY_ENVIRONMENT_ID,
    railwayEnvironmentName: process.env.RAILWAY_ENVIRONMENT_NAME,
    railwayServiceID: process.env.RAILWAY_SERVICE_ID,
    railwayServiceName: process.env.RAILWAY_SERVICE_NAME,
    railwayDeploymentID: process.env.RAILWAY_DEPLOYMENT_ID,

    gcpProject:
      process.env.GOOGLE_CLOUD_PROJECT ??
      process.env.GCP_PROJECT ??
      process.env.GCLOUD_PROJECT,

    coolifyDomainName: process.env.COOLIFY_FQDN,
    coolifyContainerName: process.env.COOLIFY_CONTAINER_NAME,

    azureEnvName: process.env.AZURE_ENV_NAME,
  };

  // Remove keys with undefined values
  return Object.fromEntries(
    Object.entries(context).filter(([_, v]) => v !== undefined),
  ) as ServerContext;
}
