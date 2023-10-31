import type {Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {onDocumentWritten} from 'firebase-functions/v2/firestore';
import {appDataConverter} from 'mirror-schema/src/app.js';
import {APP_COLLECTION} from 'mirror-schema/src/deployment.js';
import {serverSchema} from 'mirror-schema/src/server.js';
import * as v from 'shared/src/valita.js';
import {
  SecretsCache,
  SecretsClient,
  type Secrets,
} from '../../secrets/index.js';
import {checkForAutoDeployment} from '../app/auto-deploy.function.js';
import {DEPLOYMENT_SECRETS_NAMES} from '../app/secrets.js';

/**
 * `server-autoDeploy` is triggered on all changes to `servers/...` documents,
 * including creation, updating, and deletion.
 */
export const autoDeploy = (
  firestore: Firestore,
  secretsClient: SecretsClient,
) =>
  onDocumentWritten(
    {
      document: 'servers/{serverVersion}',
      secrets: [...DEPLOYMENT_SECRETS_NAMES],
    },
    async event => {
      const secrets = new SecretsCache(secretsClient);
      const {serverVersion} = event.params;
      if (!event.data) {
        throw new Error(
          `Missing event.data for ${JSON.stringify(event.params)}`,
        );
      }
      const before = event.data.before.data();
      const after = event.data.after.data();

      const affectedChannels = getAffectedChannels(
        before ? v.parse(before, serverSchema, 'passthrough').channels : [],
        after ? v.parse(after, serverSchema, 'passthrough').channels : [],
      );
      logger.info(
        `${serverVersion}: [${before?.channels}] => [${after?.channels}]. Checking apps in [${affectedChannels}]`,
      );
      await checkAppsInChannels(firestore, secrets, [...affectedChannels]);
    },
  );

export function getAffectedChannels(
  before: string[],
  after: string[],
): string[] {
  const affectedChannels = new Set<string>(before);
  for (const channel of after) {
    if (affectedChannels.has(channel)) {
      // Channels in both before and after are unaffected.
      affectedChannels.delete(channel);
    } else {
      affectedChannels.add(channel);
    }
  }
  return [...affectedChannels];
}

const BATCH_SIZE = 50;

export async function checkAppsInChannels(
  firestore: Firestore,
  secrets: Secrets,
  channels: string[],
  batchSize = BATCH_SIZE,
): Promise<void> {
  if (channels.length === 0) {
    return; // Firestore emulator hangs on an "in" query with an empty array.
  }
  let numApps = 0;
  let query = firestore
    .collection(APP_COLLECTION)
    .withConverter(appDataConverter)
    .where('serverReleaseChannel', 'in', channels)
    .limit(batchSize);

  for (;;) {
    const batch = await query.get();
    numApps += batch.size;

    logger.info(`Checking ${batch.size} apps`);
    const results = await Promise.allSettled(
      batch.docs.map(doc =>
        checkForAutoDeployment(
          firestore,
          secrets,
          doc.id,
          doc.data(),
          doc.updateTime,
        ),
      ),
    );
    for (let i = 0; i < results.length; i++) {
      const result = results[i];
      if (result.status === 'rejected') {
        logger.error(`Error checking ${batch.docs[i].id}`, result.reason);
      }
    }
    if (batch.size < batchSize) {
      break;
    }
    query = query.startAfter(batch.docs[batch.size - 1]);
  }
  logger.info(`Finished checking ${numApps} apps in channels [${channels}]`);
}
