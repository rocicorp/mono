import type {Firestore} from '@google-cloud/firestore';
import {Analytics} from 'cloudflare-api/src/analytics.js';
import type {Storage} from 'firebase-admin/storage';
import {logger} from 'firebase-functions';
import {onSchedule} from 'firebase-functions/v2/scheduler';
import {ALL_DATASETS} from 'mirror-schema/src/datasets.js';
import {
  PROVIDER_COLLECTION,
  providerDataConverter,
} from 'mirror-schema/src/provider.js';
import {datasetArchiveBucketName} from '../../config/index.js';
import {backupWeekBefore} from '../../metrics/backup.js';
import {
  SecretsCache,
  apiTokenName,
  type SecretsClient,
} from '../../secrets/index.js';

export const backup = (
  firestore: Firestore,
  storage: Storage,
  secretsClient: SecretsClient,
) =>
  // https://cloud.google.com/appengine/docs/flexible/scheduling-jobs-with-cron-yaml#custom-interval
  onSchedule('every tuesday 04:00', async event => {
    const secrets = new SecretsCache(secretsClient);
    const now = new Date(event.scheduleTime);

    const providers = await firestore
      .collection(PROVIDER_COLLECTION)
      .withConverter(providerDataConverter)
      .get();
    for (const doc of providers.docs) {
      const provider = doc.id;
      const {accountID} = doc.data();
      const apiToken = await secrets.getSecretPayload(apiTokenName(provider));
      const analytics = new Analytics({apiToken, accountID});
      const bucket = storage.bucket(datasetArchiveBucketName);

      for (const dataset of ALL_DATASETS) {
        logger.info(`Backing up ${accountID}/${dataset}`);
        try {
          await backupWeekBefore(now, analytics, dataset, bucket);
        } catch (e) {
          // Let the errorReporter surface the problem but process the remaining datasets.
          logger.error(`Error backing up ${accountID}/${dataset}`, e);
        }
      }
    }
  });
