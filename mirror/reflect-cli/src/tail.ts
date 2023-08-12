import {Queue} from 'shared/src/queue.js';
import {mustReadAppConfig} from './app-config.js';
import {authenticate} from './auth-config.js';
import {Firestore, getFirestore} from './firebase.js';
import {getApp} from './init.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {createTail} from 'mirror-protocol/src/tail.js';

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
  const idToken = await user.getIdToken();
  console.log({appID, userID});
  const app = await getApp(firestore, appID);
  console.log(app);
  console.log('Requesting create-tail');
  const tailEventSource = await createTail(appID, idToken);

  // type QueueItem =
  // | {type: 'data'; data: string}
  // | {type: 'ping'}
  // | {type: 'close'};

  const q = new Queue<string>();
  tailEventSource.onmessage = event => q.enqueue(event.data);
  for (;;) {
    const item = await q.dequeue();
    console.log(item);
  }
}
