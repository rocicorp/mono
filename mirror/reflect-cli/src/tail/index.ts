import type {TailRequest} from 'mirror-protocol/src/tail.js';
import {mustReadAppConfig} from '../app-config.js';
import {authenticate} from '../auth-config.js';
import {makeRequester} from '../requester.js';
import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';
import {TailMessage, createTailEventSource} from './tail-event-source.js';

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

  const tailEventSource = createTailEventSource(
    'app-tail',
    appID,
    idToken,
    data,
  );

  for await (const entry of tailEventSource) {
    logTailMessage(entry);
  }
}

export function logTailMessage(entry: TailMessage) {
  switch (entry.level) {
    case 'debug':
    case 'error':
    case 'info':
    case 'log':
    case 'warn':
      console[entry.level](...entry.message);
      break;
    default:
      console.log(`(${entry.level})`, ...entry.message);
  }
}
