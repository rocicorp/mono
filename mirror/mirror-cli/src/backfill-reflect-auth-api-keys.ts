import {randomBytes} from 'crypto';
import {getFirestore} from 'firebase-admin/firestore';
import {
  APP_COLLECTION,
  appDataConverter,
  ENCRYPTION_KEY_SECRET_NAME,
} from 'mirror-schema/src/app.js';
import {encryptUtf8} from 'mirror-schema/src/crypto.js';
import {getSecret} from './secrets.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

const REFLECT_AUTH_API_KEY = 'REFLECT_AUTH_API_KEY';

export function backfillReflectAuthApiKeyOptions(yargs: CommonYargsArgv) {
  return yargs.option('dry-run', {
    desc: 'Print what would be done but do not commit.',
    type: 'boolean',
    default: true,
  });
}

type BackfillReflectAuthApiKeyHandlerArgs = YargvToInterface<
  ReturnType<typeof backfillReflectAuthApiKeyOptions>
>;

export async function backfillReflectAuthApiKeyHandler(
  yargs: BackfillReflectAuthApiKeyHandlerArgs,
) {
  const {stack, dryRun} = yargs;
  const encryptionKey = await getSecret(stack, ENCRYPTION_KEY_SECRET_NAME);
  const firestore = getFirestore();
  const appsUpdate = await firestore.runTransaction(async txn => {
    const apps = await txn.get(
      firestore.collection(APP_COLLECTION).withConverter(appDataConverter),
    );

    let appsUpdated = 0;

    apps.docs.forEach(doc => {
      const data = doc.data();
      if (data.secrets?.[REFLECT_AUTH_API_KEY]) {
        console.log(`App ${doc.id} already has a REFLECT_AUTH_API_KEY`);
        return;
      }

      const randomKey = randomBytes(32).toString('base64url');
      const encryptedBytes = encryptUtf8(
        randomKey,
        Buffer.from(encryptionKey.payload, 'base64url'),
        {version: encryptionKey.version},
      );
      txn.update(doc.ref, `secrets.${REFLECT_AUTH_API_KEY}`, encryptedBytes);
      appsUpdated++;
    });

    if (dryRun) {
      throw new Error(
        `Would have updated ${appsUpdated} apps. Set --dry-run=false to commit.`,
      );
    }
    return appsUpdated;
  });
  console.log(`${appsUpdate} apps updated`);
}
