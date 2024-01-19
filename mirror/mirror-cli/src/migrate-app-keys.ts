import {DocumentReference, getFirestore} from 'firebase-admin/firestore';
import {
  apiKeyDataConverter,
  apiKeyPath,
  apiKeySchema,
  type ApiKey,
} from 'mirror-schema/src/api-key.js';
import {APP_COLLECTION, appDataConverter} from 'mirror-schema/src/app.js';
import {firestoreDataConverter} from 'mirror-schema/src/converter.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

const legacyAppKeySchema = apiKeySchema.omit('apps');
const legacyAppKeyDataConverter = firestoreDataConverter(legacyAppKeySchema);

export function migrateAppKeysOptions(yargs: CommonYargsArgv) {
  return yargs.option('dry-run', {
    desc: 'Print what would be done but do not commit.',
    type: 'boolean',
    default: true,
  });
}

type MigrateAppKeysHandlerArgs = YargvToInterface<
  ReturnType<typeof migrateAppKeysOptions>
>;

export async function migrateAppKeysHandler(yargs: MigrateAppKeysHandlerArgs) {
  const firestore = getFirestore();
  await firestore.runTransaction(async txn => {
    const teamKeys = new Map<string, ApiKey | null>();
    const appKeyDocs: DocumentReference[] = [];
    const apps = await txn.get(
      firestore.collection(APP_COLLECTION).withConverter(appDataConverter),
    );
    let collisions = 0;
    for (const doc of apps.docs) {
      const {id: appID} = doc;
      const {teamID, name} = doc.data();

      const appKeys = await txn.get(
        doc.ref.collection('keys').withConverter(legacyAppKeyDataConverter),
      );

      for (const keyDoc of appKeys.docs) {
        appKeyDocs.push(keyDoc.ref);
        const key = keyDoc.data();
        // One-off to handle the key that has the same name in by two different apps.
        const keyName =
          keyDoc.id === 'publish-key-20231217'
            ? `${name}-${keyDoc.id}`
            : keyDoc.id;
        const keyPath = apiKeyPath(teamID, keyName);
        if (teamKeys.has(keyPath)) {
          collisions++;
          console.error(
            `More than one appKey resolves to apiKeyPath ${keyPath}`,
          );
        }
        teamKeys.set(keyPath, {...key, apps: [appID]});
      }
    }

    if (collisions) {
      throw new Error('Aborted because of collisions');
    }

    for (const [path, key] of teamKeys) {
      console.info(`Creating API key ${path}`, key);
      txn.create(firestore.doc(path).withConverter(apiKeyDataConverter), key);
    }
    for (const doc of appKeyDocs) {
      txn.delete(doc);
    }
    if (yargs.dryRun) {
      throw new Error(
        `Aborted migration of ${teamKeys.size} API keys. Set --dry-run=false to commit.`,
      );
    } else {
      console.info(`Migrated ${teamKeys.size} API keys`);
    }
  });
}
