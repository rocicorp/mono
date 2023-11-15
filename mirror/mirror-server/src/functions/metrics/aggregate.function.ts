import type {Firestore} from '@google-cloud/firestore';
import {Analytics} from 'cloudflare-api/src/analytics.js';
import {logger} from 'firebase-functions';
import {onSchedule} from 'firebase-functions/v2/scheduler';
import {
  PROVIDER_COLLECTION,
  providerDataConverter,
} from 'mirror-schema/src/provider.js';
import {aggregateHourBefore} from '../../metrics/aggregate.js';
import {
  SecretsCache,
  apiTokenName,
  type SecretsClient,
} from '../../secrets/index.js';

export const aggregate = (firestore: Firestore, secretsClient: SecretsClient) =>
  onSchedule(
    {
      // Every hour on the 1st, 5th, and 30th minute, the first to compute results
      // aggressively, and the second to catch any results due to delays in Workers
      // Analytics Engine. The last aggregation is always expected to be a no-op, but
      // performed to see if are pathological scenarios in which delays last longer than
      // 5 minutes. The mirror-cli's `backfill-metrics` command can be used for one-off
      // delays / outages.
      schedule: '1,5,30 * * * *',
      retryCount: 3,
    },
    async event => {
      const secrets = new SecretsCache(secretsClient);
      const scheduleTime = new Date(event.scheduleTime);

      const providers = await firestore
        .collection(PROVIDER_COLLECTION)
        .withConverter(providerDataConverter)
        .get();

      let failure;
      for (const doc of providers.docs) {
        const provider = doc.id;
        const {accountID} = doc.data();
        const apiToken = await secrets.getSecretPayload(apiTokenName(provider));
        const analytics = new Analytics({apiToken, accountID});

        const updates = await aggregateHourBefore(
          firestore,
          analytics,
          scheduleTime,
        );
        let updated = 0;
        let unchanged = 0;

        for (const update of updates) {
          if (update.status === 'rejected') {
            failure = update.reason;
          } else if (update.value) {
            updated++;
          } else {
            unchanged++;
          }
        }

        if (scheduleTime.getUTCMinutes() >= 30 && updated > 0) {
          // Log an error so that it gets reported.
          logger.error(
            new Error(
              `Updated metrics for ${updated} apps more than 30 minutes after the hour`,
            ),
          );
        } else {
          logger.info(
            `Updated metrics for ${updated} apps (${unchanged} unchanged)`,
          );
        }
      }
      if (failure) {
        throw failure; // Throw errors so that retries kick in.
      }
    },
  );
