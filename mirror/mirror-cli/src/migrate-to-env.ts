import {
  FieldValue,
  Firestore,
  getFirestore,
  PartialWithFieldValue,
  Timestamp,
} from 'firebase-admin/firestore';
import {APP_COLLECTION} from 'mirror-schema/src/app.js';
import {firestoreDataConverter} from 'mirror-schema/src/converter.js';
import {
  APP_DEPLOYMENTS_COLLECTION_ID,
  deploymentOptionsSchema,
} from 'mirror-schema/src/deployment.js';
import {
  DEFAULT_ENV,
  envDataConverter,
  envPath,
  secretsSchema,
} from 'mirror-schema/src/env.js';
import {timestampSchema} from 'mirror-schema/src/timestamp.js';
import * as v from 'shared/src/valita.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function migrateToEnvOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('dry-run', {
      desc: 'Print what would be done but do not commit.',
      type: 'boolean',
      default: true,
    })
    .option('cleanup', {
      desc: 'Run the post-migration cleanup (after functions have been pushed) to remove obsolete fields from the App.',
      type: 'boolean',
      default: false,
    });
}

type MigrateToEnvOptions = YargvToInterface<
  ReturnType<typeof migrateToEnvOptions>
>;

const hybridDeploymentSchema = v.object({
  spec: v.object({
    envUpdateTime: timestampSchema.optional(),
  }),
});

const hybridAppSchema = v.object({
  // Legacy fields
  deploymentOptions: deploymentOptionsSchema.optional(),
  secrets: secretsSchema.optional(),

  // New field
  envUpdateTime: timestampSchema.optional(),

  runningDeployment: hybridDeploymentSchema.optional(),
});

const NULL_ENV_UPDATE_TIME = Timestamp.fromMillis(0);

export async function migrateToEnvHandler(yargs: MigrateToEnvOptions) {
  const {cleanup, dryRun} = yargs;
  const firestore = getFirestore();

  if (cleanup) {
    return deleteLegacyFields(firestore, dryRun);
  }

  await firestore.runTransaction(async txn => {
    const [apps, deployments] = await Promise.all([
      txn.get(
        firestore
          .collection(APP_COLLECTION)
          .withConverter(firestoreDataConverter(hybridAppSchema)),
      ),
      txn.get(
        firestore
          .collectionGroup(APP_DEPLOYMENTS_COLLECTION_ID)
          .withConverter(firestoreDataConverter(hybridDeploymentSchema)),
      ),
    ]);
    apps.docs.forEach(doc => {
      const {deploymentOptions, secrets, envUpdateTime, runningDeployment} =
        doc.data();
      if (envUpdateTime) {
        console.log(`App ${doc.id} is already migrated.`);
        return;
      }
      const envDoc = firestore
        .doc(envPath(doc.id, DEFAULT_ENV))
        .withConverter(envDataConverter);

      const env = {
        deploymentOptions,
        secrets,
      };
      console.log(`Creating new Env doc at ${envDoc.path}`, env);
      txn.create(envDoc, env);

      // Add the new envUpdateTime field to the App and, if present, the runningDeployment.spec.
      const appUpdate: PartialWithFieldValue<v.Infer<typeof hybridAppSchema>> =
        {
          envUpdateTime: NULL_ENV_UPDATE_TIME,
        };
      const mergeFields = ['envUpdateTime'];
      if (runningDeployment) {
        appUpdate.runningDeployment = {
          spec: {envUpdateTime: NULL_ENV_UPDATE_TIME},
        };
        mergeFields.push('runningDeployment.spec.envUpdateTime');
      }
      console.log(`Updating App ${doc.id}`, appUpdate);
      txn.set(doc.ref, appUpdate, {mergeFields});
    });
    deployments.docs.forEach(doc => {
      const {
        spec: {envUpdateTime},
      } = doc.data();
      if (envUpdateTime) {
        console.log(`Deployment ${doc.ref.path} is already migrated`);
        return;
      }
      console.log(`Adding envUpdateTime to ${doc.ref.path}`);
      txn.set(
        doc.ref,
        {spec: {envUpdateTime: NULL_ENV_UPDATE_TIME}},
        {mergeFields: ['spec.envUpdateTime']},
      );
    });

    console.log(`${apps.size} apps and ${deployments.size} deployments`);
    if (dryRun) {
      throw new Error('Aborted. Set --dry-run=false to commit.');
    }
  });
}

async function deleteLegacyFields(firestore: Firestore, dryRun: boolean) {
  await firestore.runTransaction(async txn => {
    const apps = await txn.get(
      firestore
        .collection(APP_COLLECTION)
        .withConverter(firestoreDataConverter(hybridAppSchema)),
    );
    apps.docs.forEach(doc => {
      if (doc.data().deploymentOptions === undefined) {
        console.log(`App ${doc.id} is already cleaned up.`);
        return;
      }
      console.log(`Deleting legacy fields in App ${doc.id}`);
      txn.update(doc.ref, {
        deploymentOptions: FieldValue.delete(),
        secrets: FieldValue.delete(),
      });
    });
    if (dryRun) {
      throw new Error('Aborted. Set --dry-run=false to commit.');
    }
  });
}
