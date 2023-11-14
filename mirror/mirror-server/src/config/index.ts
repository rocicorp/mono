import type {AppOptions} from 'firebase-admin';
import {logger} from 'firebase-functions';
import type {HttpsOptions} from 'firebase-functions/v2/https';

function createAppOptions(): AppOptions {
  const options = JSON.parse(process.env.FIREBASE_CONFIG || '{}') as AppOptions;
  options.projectId ??= 'unknown-project';
  options.serviceAccountId = `functions@${options.projectId}.iam.gserviceaccount.com`;
  logger.info('Initializing Firebase with', options);
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
