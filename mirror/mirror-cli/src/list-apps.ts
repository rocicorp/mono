import {getFirestore} from 'firebase-admin/firestore';
import {APP_COLLECTION, appDataConverter} from 'mirror-schema/src/app.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function listAppsOptions(yargs: CommonYargsArgv) {
  return yargs;
}

type ListAppsHandlerArgs = YargvToInterface<ReturnType<typeof listAppsOptions>>;

export async function listAppsHandler(_: ListAppsHandlerArgs) {
  const firestore = getFirestore();
  const apps = await firestore
    .collection(APP_COLLECTION)
    .withConverter(appDataConverter)
    .get();
  let i = 0;
  for (const doc of apps.docs) {
    const app = doc.data();
    if (app.runningDeployment) {
      i++;
      const pad = ' '.repeat(3 - i.toString().length);
      console.log(`${pad}${i}: ${app.runningDeployment.spec.hostname}`);
    }
  }
}
