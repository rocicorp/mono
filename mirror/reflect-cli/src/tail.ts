import {Queue} from 'shared/src/queue.js';
import {mustReadAppConfig} from './app-config.js';
import {authenticate} from './auth-config.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {createTail, CreateTailRequest} from 'mirror-protocol/src/tail.js';
import { makeRequester } from './requester.js';

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

  const data: CreateTailRequest = {
    requester: makeRequester(user.uid),
    appID,
  };

  const tailEventSource = await createTail(appID, idToken, data);

  // type QueueItem =
  // | {type: 'data'; data: string}
  // | {type: 'ping'}
  // | {type: 'close'};

  //todo(Cesar): handle tail disconnect when loop is over;
  const q = new Queue<string>();
  tailEventSource.onmessage = (event: { data: string; }) => q.enqueue(event.data);
  for (;;) {
    const item = await q.dequeue();
    console.log(item);
  }
}
