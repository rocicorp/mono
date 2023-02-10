import {Upstream, upstreamSchema} from '../protocol/up.js';
import type {ClientID, ClientMap, Socket} from '../types/client-state.js';
import type {LogContext} from '@rocicorp/logger';
import {sendError} from '../util/socket.js';
import {handlePush, type ProcessUntilDone} from './push.js';
import {handlePing} from './ping.js';
import {superstructAssert} from '../util/superstruct.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import type {PendingMutationMap} from '../types/mutation.js';

/**
 * Handles an upstream message coming into the server by dispatching to the
 * appropriate handler.
 */
export async function handleMessage(
  lc: LogContext,
  storage: DurableStorage,
  clients: ClientMap,
  pendingMutations: PendingMutationMap,
  clientID: ClientID,
  data: string,
  ws: Socket,
  processUntilDone: ProcessUntilDone,
) {
  let message;
  try {
    message = getMessage(data);
  } catch (e) {
    lc.info?.('invalid message', e);
    sendError(ws, String(e));
    return;
  }

  const client = clients.get(clientID);
  if (!client) {
    lc.error?.('client not found, closing socket');
    sendError(ws, `no such client: ${clientID}`);
    // This is not expected to ever occur.  However if it does no pushes will
    // ever succeed over this connection since it is missing an entry in
    // ClientMap.  Close connection so client can try to reconnect and recover.
    ws.close();
    return;
  }

  switch (message[0]) {
    case 'ping':
      handlePing(lc, ws);
      break;
    case 'push':
      await handlePush(
        lc,
        storage,
        client,
        clients,
        pendingMutations,
        message[1],
        () => Date.now(),
        processUntilDone,
      );
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
