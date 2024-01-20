import {FieldValue, getFirestore} from 'firebase-admin/firestore';
import {
  API_KEY_COLLECTION_ID,
  apiKeySchema,
} from 'mirror-schema/src/api-key.js';
import {firestoreDataConverter} from 'mirror-schema/src/converter.js';
import * as v from 'shared/src/valita.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

const legacyApiKeySchema = apiKeySchema.omit('appIDs').extend({
  apps: v.array(v.string()).optional(),
  appIDs: v.array(v.string()).optional(),
});
const legacyAppKeyDataConverter = firestoreDataConverter(legacyApiKeySchema);

export function migrateApiKeysOptions(yargs: CommonYargsArgv) {
  return yargs.option('dry-run', {
    desc: 'Print what would be done but do not commit.',
    type: 'boolean',
    default: true,
  });
}

type MigrateApiKeysHandlerArgs = YargvToInterface<
  ReturnType<typeof migrateApiKeysOptions>
>;

export async function migrateApiKeysHandler(yargs: MigrateApiKeysHandlerArgs) {
  const firestore = getFirestore();
  await firestore.runTransaction(async txn => {
    const keys = await txn.get(
      firestore
        .collectionGroup(API_KEY_COLLECTION_ID)
        .withConverter(legacyAppKeyDataConverter),
    );
    let num = 0;
    for (const doc of keys.docs) {
      const {apps, appIDs} = doc.data();
      if (appIDs !== undefined) {
        console.info(`Key ${doc.ref.path} is already migrated.`);
      } else if (apps === undefined) {
        throw new Error(`Key ${doc.ref.path} has no "apps" or "appIDs" field`);
      } else {
        num++;
        console.info(`Migrating key ${doc.id}`);
        txn.update(doc.ref, {
          appIDs: apps,
          apps: FieldValue.delete(),
        });
      }
    }
    if (yargs.dryRun) {
      throw new Error(
        `Aborted migration of ${num} API keys. Set --dry-run=false to commit.`,
      );
    }
    console.info(`Migrated ${num} API keys`);
  });
}
