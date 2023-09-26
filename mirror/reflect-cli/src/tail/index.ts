import type {TailRequest} from 'mirror-protocol/src/tail.js';
import {ensureAppInstantiated} from '../app-config.js';
import {authenticate} from '../auth-config.js';
import {makeRequester} from '../requester.js';
import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';
import {TailMessage, createTailEventSource} from './tail-event-source.js';

export function tailOptions(yargs: CommonYargsArgv) {
  return yargs.option('room-id', {
    describe: 'The room ID of the room to tail',
    type: 'string',
    requiresArg: true,
    demandOption: true,
  });
}

type TailHandlerArgs = YargvToInterface<ReturnType<typeof tailOptions>>;

export async function tailHandler(yargs: TailHandlerArgs) {
  const {appID} = await ensureAppInstantiated(yargs);
  const {userID, getIdToken} = await authenticate(yargs);
  const idToken = await getIdToken();
  const {roomId: roomID} = yargs;

  const data: TailRequest = {
    requester: makeRequester(userID),
    appID,
    roomID,
  };

  const tailEventSource = createTailEventSource(
    'app-tail',
    appID,
    idToken,
    data,
  );

  try {
    console.log(`Connecting to room ${roomID} to tail log...`);
    for await (const entry of tailEventSource) {
      logTailMessage(entry);
    }
  } catch (e) {
    if (e instanceof Error) {
      console.error(e.message);
      process.exit(1);
    }
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
