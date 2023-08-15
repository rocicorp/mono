import {Queue} from 'shared/src/queue.js';
import {mustReadAppConfig} from './app-config.js';
import {authenticate} from './auth-config.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {createTail} from 'mirror-protocol/src/tail.js';

export function tailOptions(yargs: CommonYargsArgv) {
  return yargs;
}

type TailHandlerArgs = YargvToInterface<ReturnType<typeof tailOptions>>;

export async function tailHandler(
  _yargs: TailHandlerArgs,
  configDirPath?: string | undefined,
) {
  configDirPath = "/Users/cesar/code/cesartesta"
  const {appID} = mustReadAppConfig(configDirPath);
  const user = await authenticate();
  const idToken = await user.getIdToken();
  const tailEventSource = await createTail(appID, idToken);

  // type QueueItem =
  // | {type: 'data'; data: string}
  // | {type: 'ping'}
  // | {type: 'close'};

  //todo(Cesar): handle tail disconnect when loop is over;
  const q = new Queue<string>();
  tailEventSource.onmessage = event => q.enqueue(event.data);
  for (;;) {
    const item = await q.dequeue();
    console.log(item);
  }
}
