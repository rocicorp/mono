import type {AppOptions} from 'firebase-admin';
import {logger} from 'firebase-functions';

function createAppOptions(): AppOptions {
  const options = JSON.parse(process.env.FIREBASE_CONFIG || '{}') as AppOptions;
  options.projectId = options.projectId ?? 'unknown-project';
  options.serviceAccountId = `functions@${options.projectId}.iam.gserviceaccount.com`;
  logger.info('Initializing Firebase with', options);
  return options;
}

export const appOptions = createAppOptions();
export const projectId = appOptions.projectId ?? '';
export const serviceAccountId = appOptions.serviceAccountId ?? '';
export const serversBucketName = `${projectId}-servers`;
