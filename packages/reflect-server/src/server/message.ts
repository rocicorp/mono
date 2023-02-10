import {Upstream, upstreamSchema} from '../protocol/up.js';
import type {ClientID, ClientMap, Socket} from '../types/client-state.js';
import type {LogContext} from '@rocicorp/logger';
import {sendError, closeWithError} from '../util/socket.js';
import {handlePush, type ProcessUntilDone} from './push.js';
import {handlePing} from './ping.js';
import {superstructAssert} from '../util/superstruct.js';
import {ErrorKind} from '../protocol/error.js';

/**
 * Handles an upstream message coming into the server by dispatching to the
 * appropriate handler.
 */
export function handleMessage(
  lc: LogContext,
  clientMap: ClientMap,
  clientID: ClientID,
  data: string,
  ws: Socket,
  processUntilDone: ProcessUntilDone,
) {
  let message;
  try {
    message = getMessage(data);
  } catch (e) {
    sendError(lc, ws, ErrorKind.InvalidMessage, String(e));
    return;
  }

  const client = clientMap.get(clientID);
  if (!client) {
    // This is not expected to ever occur.  However if it does no pushes will
    // ever succeed over this connection since it is missing an entry in
    // ClientMap.  Close connection so client can try to reconnect and recover.
    closeWithError(lc, ws, ErrorKind.ClientNotFound, clientID);
    return;
  }

  switch (message[0]) {
    case 'ping':
      handlePing(lc, ws);
      break;
    case 'push':
      handlePush(lc, client, message[1], () => Date.now(), processUntilDone);
      break;
    default:
      throw new Error(`Unknown message type: ${message[0]}`);
  }
}

function getMessage(data: string): Upstream {
  const value = JSON.parse(data);
  superstructAssert(value, upstreamSchema);
  return value;
}
