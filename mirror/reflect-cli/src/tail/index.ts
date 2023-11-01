import {
  TailMessage,
  connectedMessageSchema,
  errorMessageSchema,
} from 'mirror-protocol/src/tail-message.js';
import type {RoomTailRequest} from 'mirror-protocol/src/tail.js';
import * as valita from 'shared/src/valita.js';
import {ensureAppInstantiated} from '../app-config.js';
import {authenticate} from '../auth-config.js';
import {makeRequester} from '../requester.js';
import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';
import {createTailEventSource} from './tail-event-source.js';

export function tailOptions(yargs: CommonYargsArgv) {
  return yargs.option('room', {
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
  const {room: roomID} = yargs;

  const data: RoomTailRequest = {
    requester: makeRequester(userID),
    appID,
    roomID,
  };

  const tailEventSource = createTailEventSource(
    'room-tail',
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
      if (/\b404\b/.test(e.message)) {
        console.error('404 Not found');
        console.error('Could not connect to room to tail log.');
        console.error(
          'Please update your app dependencies to @rocicorp/reflect@latest.',
        );
        return;
      }
    }
    throw e;
  }
}

function logTailMessage(entry: TailMessage) {
  if (valita.is(entry, connectedMessageSchema)) {
    console.log('Connected.');
    return;
  }

  if (valita.is(entry, errorMessageSchema)) {
    // failed to connect
    console.error(`${entry.kind}: ${entry.message}`);
    process.exit(1);
  }

  const {level, message} = entry;
  switch (level) {
    case 'debug':
    case 'error':
    case 'info':
    case 'log':
    case 'warn':
      console[level](...message);
      break;
    default:
      console.log(`(${level})`, ...message);
  }
}
