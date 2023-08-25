import {Queue} from 'shared/src/queue.js';
import {mustReadAppConfig} from './app-config.js';
import {authenticate} from './auth-config.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {tail, TailRequest} from 'mirror-protocol/src/tail.js';
import {makeRequester} from './requester.js';

export function tailOptions(yargs: CommonYargsArgv) {
  return yargs;
}

type TailHandlerArgs = YargvToInterface<ReturnType<typeof tailOptions>>;

export async function tailHandler(
  _yargs: TailHandlerArgs,
  configDirPath?: string | undefined,
) {
  const {appID} = mustReadAppConfig(configDirPath);
  const user = await authenticate();
  const idToken = await user.getIdToken();

  const data: TailRequest = {
    requester: makeRequester(user.uid),
    appID,
  };

  const tailEventSource = await tail(appID, idToken, data);
  const q = new Queue<string>();
  tailEventSource.onMessage = async (message: string) => {
    await q.enqueue(message);
  };
  void tailEventSource.startListening();
  for (;;) {
    const item = await q.dequeue();
    console.log(item);
  }
}
