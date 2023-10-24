import {getFirestore} from 'firebase-admin/firestore';
import {APP_COLLECTION, appDataConverter} from 'mirror-schema/src/app.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function listDeployedAppsOptions(yargs: CommonYargsArgv) {
  return yargs;
}

type ListDeployedAppsHandlerArgs = YargvToInterface<
  ReturnType<typeof listDeployedAppsOptions>
>;

export async function listDeployedAppsHandler(_: ListDeployedAppsHandlerArgs) {
  const firestore = getFirestore();
  const apps = await firestore
    .collection(APP_COLLECTION)
    .orderBy('runningDeployment.deployTime')
    .withConverter(appDataConverter)
    .get();
  let i = 0;
  for (const doc of apps.docs) {
    const {runningDeployment} = doc.data();
    if (runningDeployment) {
      i++;
      const pad = ' '.repeat(3 - i.toString().length);
      const time = new Date(
        1000 * (runningDeployment.deployTime?.seconds ?? 0),
      );
      console.log(
        `${pad}${i}: ${time.toISOString()} ${runningDeployment.spec.hostname}`,
      );
    }
  }
}
