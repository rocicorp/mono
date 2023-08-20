import {getFirestore} from 'firebase-admin/firestore';
import {APP_DEPLOYMENTS_COLLECTION_ID} from 'mirror-schema/src/deployment.js';
import type {
  CommonYargsArgv,
  YargvToInterface,
} from 'reflect-cli/src/yarg-types.js';
import {assert} from 'shared/src/asserts.js';

export function wipeDeploymentsOptions(yargs: CommonYargsArgv) {
  return yargs.option('forrealz', {
    describe: 'Must be specified because this is a dangerous operation',
    type: 'boolean',
  });
}

type WipeDeploymentsHandlerArgs = YargvToInterface<
  ReturnType<typeof wipeDeploymentsOptions>
>;

export async function wipeDeploymentsHandler(
  yargs: WipeDeploymentsHandlerArgs,
) {
  assert(yargs.stack === 'staging', 'This command is only allowed in staging');
  if (!yargs.forrealz) {
    throw new Error(
      'Must specify --forrealz to confirm that you really want to wipe all deployments.',
    );
  }

  const firestore = getFirestore();
  const deployments = await firestore
    .collectionGroup(APP_DEPLOYMENTS_COLLECTION_ID)
    .select()
    .get();
  let batch = firestore.batch();
  let size = 0;
  for (const doc of deployments.docs) {
    batch.delete(doc.ref);
    size++;
    if (size === 500) {
      console.info(`Deleting ${size} Deployments`);
      await batch.commit();
      batch = firestore.batch();
      size = 0;
    }
  }
  console.info(`Deleting ${size} Deployments`);
  await batch.commit();
}
