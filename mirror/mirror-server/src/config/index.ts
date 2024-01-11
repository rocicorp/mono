import type {AppOptions} from 'firebase-admin';
import {logger} from 'firebase-functions';
import type {HttpsOptions} from 'firebase-functions/v2/https';

function createAppOptions(): AppOptions {
  const options = JSON.parse(process.env.FIREBASE_CONFIG || '{}') as AppOptions;
  options.projectId ??= 'unknown-project';
  options.serviceAccountId = `functions@${options.projectId}.iam.gserviceaccount.com`;
  logger.debug(
    `Initializing Firebase from ${process.env.FIREBASE_CONFIG}`,
    options,
  );
  return options;
}

export const appOptions = createAppOptions();
export const {projectId = ''} = appOptions;
export const {serviceAccountId = ''} = appOptions;
export const modulesBucketName = `${projectId}-modules`;
export const datasetArchiveBucketName = `${projectId}-dataset-archive`;

export const baseHttpsOptions: HttpsOptions = {
  // TODO(darick): Convert to a limited list.
  cors: true,
};

export function cloudFunctionURL(functionName: string): string {
  return projectId === 'unknown-project'
    ? `http://127.0.0.1:5001/${functionName}` // Assume emulator
    : `https://us-central1-${projectId}.cloudfunctions.net/${functionName}`;
}
