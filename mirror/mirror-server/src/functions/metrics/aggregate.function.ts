import {Analytics} from 'cloudflare-api/src/analytics.js';
import {
  DocumentReference,
  FieldValue,
  type Firestore,
  type QueryDocumentSnapshot,
} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {onSchedule} from 'firebase-functions/v2/scheduler';
import {
  AGGREGATIONS,
  Aggregation,
  aggregationDataConverter,
  aggregationPath,
} from 'mirror-schema/src/metrics.js';
import {
  PROVIDER_COLLECTION,
  Provider,
  providerDataConverter,
} from 'mirror-schema/src/provider.js';
import {sleep} from 'shared/src/sleep.js';
import {aggregateHourBefore} from '../../metrics/aggregate.js';
import {
  Secrets,
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
      // 5 minutes. The mirror-cli's `backfill-metrics` command can be used for one-offs.
      //
      // To recover from outages, each aggregation first records the attempt in Firestore,
      // deleting it if successful. Unsuccessful (i.e. undeleted) attempts are retried
      // on the next scheduled aggregation.
      schedule: '1,5,30 * * * *',
      retryCount: 3,
    },
    async event => {
      const secrets = new SecretsCache(secretsClient);
      const scheduleTime = new Date(event.scheduleTime);

      // First, record the Aggregation in Firestore so that it can be retried upon a failure.
      await firestore
        .doc(
          aggregationPath(
            new Date(
              Date.UTC(
                scheduleTime.getUTCFullYear(),
                scheduleTime.getUTCMonth(),
                scheduleTime.getUTCDate(),
                scheduleTime.getUTCHours(),
              ),
            ),
          ),
        )
        .withConverter(aggregationDataConverter)
        .set({lastAttempt: FieldValue.serverTimestamp()});

      const [providers, aggregations] = await Promise.all([
        firestore
          .collection(PROVIDER_COLLECTION)
          .withConverter(providerDataConverter)
          .get(),
        firestore
          .collection(AGGREGATIONS)
          .withConverter(aggregationDataConverter)
          .listDocuments(),
      ]);
      if (aggregations.length > 1) {
        logger.debug(`Retrying ${aggregations.length - 1} past aggregations`);
      }

      for (let i = 0; i < aggregations.length; i++) {
        if (i > 0) {
          await sleep(1000);
        }
        await aggregateAllProviders(
          firestore,
          secrets,
          providers.docs,
          aggregations[i],
        );
      }
      logger.info('All aggregations successful');
    },
  );

async function aggregateAllProviders(
  firestore: Firestore,
  secrets: Secrets,
  providers: QueryDocumentSnapshot<Provider>[],
  aggregation: DocumentReference<Aggregation>,
) {
  // Update the attempt for bookkeeping retries.
  await aggregation.set({lastAttempt: FieldValue.serverTimestamp()});
  const endTime = new Date(aggregation.id);

  let failure;
  for (const doc of providers) {
    const provider = doc.id;
    const {accountID} = doc.data();
    const apiToken = await secrets.getSecretPayload(apiTokenName(provider));
    const analytics = new Analytics({apiToken, accountID});

    const updates = await aggregateHourBefore(firestore, analytics, endTime);
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

    if (updated > 0 && Date.now() - endTime.getTime() > 1000 * 60 * 30) {
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

  // Clear the attempt if successful.
  await aggregation.delete();
}
