import {mustReadAppConfig} from './app-config.js';
import {authenticate} from './auth-config.js';
import {Firestore, getFirestore} from './firebase.js';
import {getApp} from './init.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function tailOptions(yargs: CommonYargsArgv) {
  return yargs;
}

type TailHandlerArgs = YargvToInterface<ReturnType<typeof tailOptions>>;

export async function tailHandler(
  _yargs: TailHandlerArgs,
  configDirPath?: string | undefined,
  firestore: Firestore = getFirestore(), // Overridden in tests.
) {
  const {appID} = mustReadAppConfig(configDirPath);

  const user = await authenticate();
  const userID = user.uid;

  
  console.log({appID, userID});
  const app = await getApp(firestore, appID);

  
  console.log(app);

  
  const eventSource = new EventSource(`http://127.0.0.1:5001/reflect-mirror-staging/us-central1/tail-create`);
  eventSource.onmessage = function (event) {
    console.log(event);
  }



}
