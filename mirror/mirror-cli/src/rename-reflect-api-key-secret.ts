import {FieldValue, getFirestore} from 'firebase-admin/firestore';
import {ENVS_COLLECTION_ID, envDataConverter} from 'mirror-schema/src/env.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function renameReflectApiKeySecretOptions(yargs: CommonYargsArgv) {
  return yargs.option('dry-run', {
    desc: 'Print what would be done but do not commit.',
    type: 'boolean',
    default: true,
  });
}

type RenameReflectApiKeySecretOptions = YargvToInterface<
  ReturnType<typeof renameReflectApiKeySecretOptions>
>;

export async function renameReflectApiKeySecretHandler(
  yargs: RenameReflectApiKeySecretOptions,
) {
  const {dryRun} = yargs;
  const firestore = getFirestore();

  await firestore.runTransaction(async txn => {
    const envs = await txn.get(
      firestore
        .collectionGroup(ENVS_COLLECTION_ID)
        .withConverter(envDataConverter),
    );
    envs.docs.forEach(doc => {
      const env = doc.data();
      const apiKey = env.secrets['REFLECT_AUTH_API_KEY'];
      if (!apiKey) {
        console.log(`${doc.ref.path} is already migrated`);
        return;
      }
      const update = {
        ['REFLECT_API_KEY']: apiKey,
        ['REFLECT_AUTH_API_KEY']: FieldValue.delete(),
      };
      console.log(`Update ${doc.ref.path} with `, update);
      txn.set(
        doc.ref,
        {secrets: update},
        {
          mergeFields: [
            'secrets.REFLECT_API_KEY',
            'secrets.REFLECT_AUTH_API_KEY',
          ],
        },
      );
    });

    if (dryRun) {
      throw new Error('Aborted. Set --dry-run=false to commit.');
    }
  });
}
